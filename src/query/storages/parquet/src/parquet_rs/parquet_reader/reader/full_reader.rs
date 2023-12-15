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

use std::sync::Arc;

use arrow_schema::ArrowError;
use bytes::Bytes;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_metrics::storage::metrics_inc_omit_filter_rowgroups;
use common_metrics::storage::metrics_inc_omit_filter_rows;
use futures::StreamExt;
use opendal::Operator;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;

use crate::parquet_rs::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch_by_field_paths;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;
use crate::ParquetRSPruner;

/// The reader to read a whole parquet file.
pub struct ParquetRSFullReader {
    pub(super) op: Operator,
    pub(super) schema: DataSchemaRef,
    pub(super) predicate: Option<Arc<ParquetPredicate>>,

    /// Columns to output.
    pub(super) projection: ProjectionMask,
    /// Field paths helping to traverse columns.
    ///
    /// If we use [`ProjectionMask`] to get inner columns of a struct,
    /// the columns will be contains in a struct array in the read [`RecordBatch`].
    ///
    /// Therefore, if `field_paths` is [Some],
    /// we should extract inner columns from the struct manually by traversing the nested column;
    /// if `field_paths` is [None], we can skip the traversing.
    pub(super) field_paths: Arc<Option<FieldPaths>>,

    pub(super) pruner: Option<ParquetRSPruner>,

    // Options
    pub(super) need_page_index: bool,
    pub(super) batch_size: usize,
}

impl ParquetRSFullReader {
    pub async fn prepare_data_stream(&self, loc: &str) -> Result<ParquetRecordBatchStream<Reader>> {
        let reader: Reader = self.op.reader(loc).await?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            reader,
            ArrowReaderOptions::new()
                .with_page_index(self.need_page_index)
                .with_skip_arrow_metadata(true),
        )
        .await?
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size);

        let mut all_pruned = false;

        let file_meta = builder.metadata().clone();

        // Prune row groups.
        if let Some(pruner) = &self.pruner {
            let (selected_row_groups, omits) = pruner.prune_row_groups(&file_meta, None)?;
            all_pruned = omits.iter().all(|x| *x);
            builder = builder.with_row_groups(selected_row_groups.clone());

            if !all_pruned {
                let row_selection = pruner.prune_pages(&file_meta, &selected_row_groups)?;

                if let Some(row_selection) = row_selection {
                    builder = builder.with_row_selection(row_selection);
                }
            } else {
                metrics_inc_omit_filter_rowgroups(file_meta.num_row_groups() as u64);
                metrics_inc_omit_filter_rows(file_meta.file_metadata().num_rows() as u64);
            }
        }

        if !all_pruned {
            if let Some(predicate) = self.predicate.as_ref() {
                let projection = predicate.projection().clone();
                let predicate = predicate.clone();
                let predicate_fn = move |batch| {
                    predicate
                        .evaluate(&batch)
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
                };
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                    ArrowPredicateFn::new(projection, predicate_fn),
                )]));
            }
        }

        Ok(builder.build()?)
    }

    /// Read a [`DataBlock`] from parquet file using native apache arrow-rs stream API.
    pub async fn read_block_from_stream(
        &self,
        stream: &mut ParquetRecordBatchStream<Reader>,
    ) -> Result<Option<DataBlock>> {
        let record_batch = stream.next().await.transpose()?;

        if let Some(batch) = record_batch {
            let blocks = transform_record_batch(self.schema.as_ref(), &batch, &self.field_paths)?;
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }

    /// Read a [`DataBlock`] from bytes.
    pub fn read_blocks_from_binary(&self, raw: Vec<u8>) -> Result<Vec<DataBlock>> {
        let bytes = Bytes::from(raw);
        let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes,
            ArrowReaderOptions::new().with_skip_arrow_metadata(true),
        )?
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size);

        // Prune row groups.
        let file_meta = builder.metadata().clone();

        let mut full_match = false;
        if let Some(pruner) = &self.pruner {
            let (selected_row_groups, omits) = pruner.prune_row_groups(&file_meta, None)?;

            full_match = omits.iter().all(|x| *x);
            builder = builder.with_row_groups(selected_row_groups.clone());

            if !full_match {
                let row_selection = pruner.prune_pages(&file_meta, &selected_row_groups)?;

                if let Some(row_selection) = row_selection {
                    builder = builder.with_row_selection(row_selection);
                }
            } else {
                metrics_inc_omit_filter_rowgroups(file_meta.num_row_groups() as u64);
                metrics_inc_omit_filter_rows(file_meta.file_metadata().num_rows() as u64);
            }
        }

        if !full_match {
            if let Some(predicate) = self.predicate.as_ref() {
                let projection = predicate.projection().clone();
                let predicate = predicate.clone();
                let predicate_fn = move |batch| {
                    predicate
                        .evaluate(&batch)
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
                };
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                    ArrowPredicateFn::new(projection, predicate_fn),
                )]));
            }
        }
        let reader = builder.build()?;
        // Write `if` outside iteration to reduce branches.
        if let Some(field_paths) = self.field_paths.as_ref() {
            reader
                .into_iter()
                .map(|batch| {
                    let batch = batch?;
                    transform_record_batch_by_field_paths(&batch, field_paths)
                })
                .collect()
        } else {
            reader
                .into_iter()
                .map(|batch| {
                    let batch = batch?;
                    Ok(DataBlock::from_record_batch(self.schema.as_ref(), &batch)?.0)
                })
                .collect()
        }
    }
}
