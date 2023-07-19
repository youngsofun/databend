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

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::parquet::metadata::FileMetaData;
use common_base::runtime::execute_futures_in_parallel;
use common_base::runtime::GLOBAL_MEM_STAT;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;

#[async_backtrace::framed]
pub async fn read_parquet_schema_async(operator: &Operator, path: &str) -> Result<ArrowSchema> {
    let mut reader = operator.reader(path).await?;
    let meta = pread::read_metadata_async(&mut reader).await.map_err(|e| {
        ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
    })?;

    infer_schema_with_extension(&meta)
}

pub fn infer_schema_with_extension(meta: &FileMetaData) -> Result<ArrowSchema> {
    let arrow_schema = pread::infer_schema(meta)?;
    // Convert data types to extension types using meta information.
    // Mainly used for types such as Variant and Bitmap,
    // as they have the same physical type as String.
    if let Some(metas) = meta.key_value_metadata() {
        let mut new_fields = arrow_schema.fields.clone();
        for (i, field) in arrow_schema.fields.iter().enumerate() {
            for meta in metas {
                if field.name == meta.key {
                    let data_type = ArrowType::Extension(
                        meta.value.clone().unwrap(),
                        Box::new(field.data_type.clone()),
                        None,
                    );
                    let new_field =
                        ArrowField::new(field.name.clone(), data_type, field.is_nullable);
                    new_fields[i] = new_field;
                    break;
                }
            }
        }
        Ok(new_fields.into())
    } else {
        Ok(arrow_schema)
    }
}

async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Vec<FileMetaData>> {
    // todo(youngsofun): we should use size in StageFileInfo, but parquet2 do not have the interface for now
    let mut metas = vec![];
    for (path, _size) in file_infos {
        let mut reader = op.reader(&path).await?;
        metas.push(pread::read_metadata_async(&mut reader).await?)
    }
    let used = GLOBAL_MEM_STAT.get_memory_usage();
    if max_memory_usage as i64 - used < 100 * 1024 * 1024 {
        Err(ErrorCode::Internal(format!(
            "not enough memory to load parquet file metas, max_memory_usage = {}, used = {}.",
            max_memory_usage, used
        )))
    } else {
        Ok(metas)
    }
}

#[async_backtrace::framed]
pub async fn read_parquet_metas_in_parallel(
    op: Operator,
    file_infos: Vec<(String, u64)>,
    thread_nums: usize,
    permit_nums: usize,
    max_memory_usage: u64,
) -> Result<Vec<FileMetaData>> {
    let batch_size = 100;
    if file_infos.len() <= batch_size {
        read_parquet_metas_batch(file_infos, op.clone(), max_memory_usage).await
    } else {
        let mut chunks = file_infos.chunks(batch_size);

        let tasks = std::iter::from_fn(move || {
            chunks.next().map(|location| {
                read_parquet_metas_batch(location.to_vec(), op.clone(), max_memory_usage)
            })
        });

        let result = execute_futures_in_parallel(
            tasks,
            thread_nums,
            permit_nums,
            "read-parquet-metas-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<Vec<_>>>>()?
        .into_iter()
        .flatten()
        .collect();

        Ok(result)
    }
}
