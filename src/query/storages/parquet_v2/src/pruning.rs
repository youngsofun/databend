// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use std::io::Seek;
use std::mem;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::arrow::io::parquet::read::get_field_pages;
use common_arrow::arrow::io::parquet::read::indexes::compute_page_row_intervals;
use common_arrow::arrow::io::parquet::read::indexes::read_columns_indexes;
use common_arrow::arrow::io::parquet::read::indexes::FieldPageStatistics;
use common_arrow::parquet::indexes::Interval;
use common_arrow::parquet::read::read_pages_locations;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::TopK;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use common_storage::read_parquet_metas_in_parallel;
use common_storage::ColumnNodes;
use opendal::Operator;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::metadata::SchemaDescriptor;
use storages_common_pruner::RangePruner;
use storages_common_pruner::RangePrunerCreator;

use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetPart;
use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_part::ParquetSmallFilesPart;
use crate::statistics::collect_row_group_stats;

/// Prune parquet row groups and pages.
pub struct PartitionPruner {
    /// Table schema.
    pub schema: TableSchemaRef,
    pub schema_descr: SchemaDescriptor,
    pub schema_from: String,
    /// Pruner to prune row groups.
    pub row_group_pruner: Option<Arc<dyn RangePruner + Send + Sync>>,
    /// The projected column indices.
    pub columns_to_read: HashSet<FieldIndex>,
    /// The projected column nodes.
    pub column_nodes: ColumnNodes,
    /// Whether to skip pruning.
    pub skip_pruning: bool,
    /// top k information from pushed down information. The usize is the offset of top k column in `schema`.
    pub top_k: Option<(TopK, usize)>,
    // TODO: use limit information for pruning
    // /// Limit of this query. If there is order by and filter, it will not be used (assign to `usize::MAX`).
    // pub limit: usize,
    pub parquet_fast_read_bytes: usize,
    pub compression_ratio: f64,
    pub max_memory_usage: u64,
}

fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if expect.fields() != actual.fields() || expect.columns() != actual.columns() {
        return Err(ErrorCode::BadBytes(format!(
            "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
            schema_from, path, expect, actual
        )));
    }
    Ok(())
}

impl PartitionPruner {
    #[async_backtrace::framed]
    pub fn read_and_prune_file_meta(
        &self,
        path: &str,
        file_meta: FileMetaData,
        operator: Operator,
    ) -> Result<(PartStatistics, Vec<ParquetRowGroupPart>)> {
        check_parquet_schema(
            &self.schema_descr,
            file_meta.schema(),
            path,
            &self.schema_from,
        )?;
        let mut stats = PartStatistics::default();
        let mut partitions = vec![];

        let is_blocking_io = operator.info().can_blocking();
        let mut row_group_pruned = vec![false; file_meta.row_groups.len()];

        let no_stats = file_meta.row_groups.iter().any(|r| {
            r.columns()
                .iter()
                .any(|c| c.metadata().statistics.is_none())
        });

        let row_group_stats = if no_stats {
            None
        } else if self.row_group_pruner.is_some() && !self.skip_pruning {
            let pruner = self.row_group_pruner.as_ref().unwrap();
            // If collecting stats fails or `should_keep` is true, we still read the row group.
            // Otherwise, the row group will be pruned.
            if let Ok(row_group_stats) =
                collect_row_group_stats(&self.column_nodes, &file_meta.row_groups)
            {
                for (idx, (stats, _rg)) in row_group_stats
                    .iter()
                    .zip(file_meta.row_groups.iter())
                    .enumerate()
                {
                    row_group_pruned[idx] = !pruner.should_keep(stats, None);
                }
                Some(row_group_stats)
            } else {
                None
            }
        } else if self.top_k.is_some() {
            collect_row_group_stats(&self.column_nodes, &file_meta.row_groups).ok()
        } else {
            None
        };

        for (rg_idx, rg) in file_meta.row_groups.iter().enumerate() {
            if row_group_pruned[rg_idx] {
                continue;
            }

            stats.read_rows += rg.num_rows();
            stats.read_bytes += rg.total_byte_size();
            stats.partitions_scanned += 1;

            // Currently, only blocking io is allowed to prune pages.
            let row_selection = if self.page_pruners.is_some()
                && is_blocking_io
                && rg.columns().iter().all(|c| {
                    c.column_chunk().column_index_offset.is_some()
                        && c.column_chunk().column_index_length.is_some()
                }) {
                let mut reader = operator.blocking().reader(path)?;
                self.page_pruners
                    .as_ref()
                    .map(|pruners| filter_pages(&mut reader, &self.schema, rg, pruners))
                    .transpose()
                    .unwrap_or(None)
            } else {
                None
            };

            let mut column_metas = HashMap::with_capacity(self.columns_to_read.len());
            for index in self.columns_to_read.iter() {
                let c = &rg.columns()[*index];
                let (offset, length) = c.byte_range();

                let min_max = self
                    .top_k
                    .as_ref()
                    .filter(|(tk, _)| tk.column_id as usize == *index)
                    .zip(row_group_stats.as_ref())
                    .map(|((_, offset), stats)| {
                        let stat = stats[rg_idx].get(&(*offset as u32)).unwrap();
                        (stat.min.clone(), stat.max.clone())
                    });

                column_metas.insert(*index, ColumnMeta {
                    offset,
                    length,
                    num_values: c.num_values(),
                    compression: c.compression(),
                    uncompressed_size: c.uncompressed_size() as u64,
                    min_max,
                    has_dictionary: c.dictionary_page_offset().is_some(),
                });
            }

            partitions.push(ParquetRowGroupPart {
                location: path.to_string(),
                num_rows: rg.num_rows(),
                column_metas,
                row_selection,
                sort_min_max: None,
            })
        }
        Ok((stats, partitions))
    }

    /// Try to read parquet meta to generate row-group-wise partitions.
    /// And prune row groups an pages to generate the final row group partitions.
    #[async_backtrace::framed]
    pub async fn read_and_prune_partitions(
        &self,
        operator: Operator,
        locations: &Vec<(String, u64)>,
    ) -> Result<(PartStatistics, Partitions)> {
        // part stats
        let mut stats = PartStatistics::default();

        let mut large_files = vec![];
        let mut small_files = vec![];
        for (location, size) in locations {
            if *size > self.parquet_fast_read_bytes as u64 {
                large_files.push((location.clone(), *size));
            } else {
                small_files.push((location.clone(), *size));
            }
        }

        let mut partitions = Vec::with_capacity(locations.len());

        let is_blocking_io = operator.info().can_blocking();

        // 1. Read parquet meta data. Distinguish between sync and async reading.
        let file_metas = if is_blocking_io {
            let mut file_metas = Vec::with_capacity(locations.len());
            for (location, _size) in &large_files {
                let mut reader = operator.blocking().reader(location)?;
                let file_meta = pread::read_metadata(&mut reader).map_err(|e| {
                    ErrorCode::Internal(format!(
                        "Read parquet file '{}''s meta error: {}",
                        location, e
                    ))
                })?;
                file_metas.push(file_meta);
            }
            file_metas
        } else {
            read_parquet_metas_in_parallel(
                operator.clone(),
                large_files.clone(),
                16,
                64,
                self.max_memory_usage,
            )
            .await?
        };

        // 2. Use file meta to prune row groups or pages.
        let mut max_compression_ratio = self.compression_ratio;
        let mut max_compressed_size = 0u64;

        // If one row group does not have stats, we cannot use the stats for topk optimization.
        for (file_id, file_meta) in file_metas.into_iter().enumerate() {
            stats.partitions_total += file_meta.row_groups.len();
            let (sub_stats, parts) = self.read_and_prune_file_meta(
                &large_files[file_id].0,
                file_meta,
                operator.clone(),
            )?;
            for p in parts {
                max_compression_ratio = max_compression_ratio
                    .max(p.uncompressed_size() as f64 / p.compressed_size() as f64);
                max_compressed_size = max_compressed_size.max(p.compressed_size());
                partitions.push(ParquetPart::RowGroup(p));
            }
            stats.partitions_total += sub_stats.partitions_total;
            stats.partitions_scanned += sub_stats.partitions_scanned;
            stats.read_bytes += sub_stats.read_bytes;
            stats.read_rows += sub_stats.read_rows;
        }

        let num_large_partitions = partitions.len();

        collect_small_file_parts(
            small_files,
            max_compression_ratio,
            max_compressed_size,
            &mut partitions,
            &mut stats,
            self.columns_to_read.len(),
        );

        tracing::info!(
            "copy {num_large_partitions} large partitions and {} small partitions.",
            partitions.len() - num_large_partitions
        );

        let partition_kind = PartitionsShuffleKind::Mod;
        let partitions = partitions
            .into_iter()
            .map(|p| p.convert_to_part_info())
            .collect();

        Ok((stats, Partitions::create_nolazy(partition_kind, partitions)))
    }
}

/// files smaller than setting fast_read_part_bytes is small file,
/// which is load in one read, without reading its meta in advance.
/// some considerations:
/// 1. to fully utilize the IO, multiple small files are loaded in one part.
/// 2. to avoid OOM, the total size of small files in one part is limited,
///    and we need compression_ratio to estimate the uncompressed size.
fn collect_small_file_parts(
    small_files: Vec<(String, u64)>,
    mut max_compression_ratio: f64,
    mut max_compressed_size: u64,
    partitions: &mut Vec<ParquetPart>,
    stats: &mut PartStatistics,
    num_columns_to_read: usize,
) {
    if max_compression_ratio <= 0.0 || max_compression_ratio >= 1.0 {
        // just incase
        max_compression_ratio = 1.0;
    }
    if max_compressed_size == 0 {
        // there are no large files, so we choose a default value.
        max_compressed_size = ((128usize << 20) as f64 / max_compression_ratio) as u64;
    }
    let mut num_small_files = small_files.len();
    stats.read_rows += num_small_files;
    let mut small_part = vec![];
    let mut part_size = 0;
    let mut make_small_files_part = |files: Vec<(String, u64)>, part_size| {
        let estimated_uncompressed_size = (part_size as f64 / max_compression_ratio) as u64;
        num_small_files -= files.len();
        partitions.push(ParquetPart::SmallFiles(ParquetSmallFilesPart {
            files,
            estimated_uncompressed_size,
        }));
        stats.partitions_scanned += 1;
        stats.partitions_total += 1;
    };
    let max_files = num_columns_to_read * 2;
    for (path, size) in small_files.into_iter() {
        stats.read_bytes += size as usize;
        if !small_part.is_empty()
            && (part_size + size > max_compressed_size || small_part.len() + 1 >= max_files)
        {
            make_small_files_part(mem::take(&mut small_part), part_size);
            part_size = 0;
        }
        small_part.push((path, size));
        part_size += size;
    }
    if !small_part.is_empty() {
        make_small_files_part(mem::take(&mut small_part), part_size);
    }
    assert_eq!(num_small_files, 0);
}