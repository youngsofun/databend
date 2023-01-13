// Copyright 2022 Datafuse Labs.
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

//! 2022-11-25:
//! TODO: support synchronize with remote
//! Note:
//! currently, we only care about immutable tables
//! once the table created we don't update it.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use futures::StreamExt;
use iceberg_rs::model::table::TableMetadataV2;
use opendal::Operator;

use crate::converters::meta_iceberg_to_databend;

/// file marking the current version of metadata file
const META_PTR: &str = "metadata/version_hint.text";

/// accessor wrapper as a table
#[allow(unused)]
pub struct IcebergTable {
    /// database that belongs to
    database: String,
    /// name of the current table
    name: String,
    /// root of the table
    tbl_root: Operator,
    /// table metadata
    manifests: TableMetadataV2,
    /// table information
    info: TableInfo,
}

impl IcebergTable {
    /// create a new table on the table directory
    pub async fn try_create_table_from_read(
        catalog: &str,
        database: &str,
        table_name: &str,
        tbl_root: Operator,
    ) -> Result<IcebergTable> {
        // detect the latest manifest file
        let latest_manifest = Self::version_detect(&tbl_root).await?;
        // get table metadata from metadata file
        let meta_file_latest = tbl_root.object(&latest_manifest);
        let meta_json = meta_file_latest.read().await.map_err(|e| {
            ErrorCode::ReadTableDataError(format!(
                "invalid metadata in {}: {:?}",
                meta_file_latest.name(),
                e
            ))
        })?;
        let metadata: TableMetadataV2 =
            serde_json::de::from_slice(meta_json.as_slice()).map_err(|e| {
                ErrorCode::ReadTableDataError(format!(
                    "invalid metadata in {}: {:?}",
                    meta_file_latest.name(),
                    e
                ))
            })?;

        // construct table info
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("IcebergTable: '{database}'.'{table_name}'"),
            name: table_name.to_string(),
            meta: meta_iceberg_to_databend(catalog, &metadata),
            ..Default::default()
        };

        // finish making table
        Ok(Self {
            database: database.to_string(),
            name: table_name.to_string(),
            tbl_root,
            manifests: metadata,
            info,
        })
    }

    /// version_detect figures out the manifest list version of the table
    /// and gives the relative path from table root directory
    /// to latest metadata json file
    async fn version_detect(tbl_root: &Operator) -> Result<String> {
        // try Dremio's way
        // Dremio has an `version_hint.txt` file
        // recording the latest snapshot version number
        // and stores metadata
        let hint = tbl_root.object(META_PTR);
        if let Ok(version_hint) = hint.read().await {
            if let Ok(version_str) = String::from_utf8(version_hint) {
                if let Ok(version) = version_str.trim().parse::<u64>() {
                    return Ok(format!("metadata/v{version}.metadata.json"));
                }
            }
        }
        // try Spark's way
        // Spark will arange all files with a sequencial number
        // in such case, we just need to find the file with largest alphabetical name.
        let meta_dir = tbl_root.object("metadata/");
        let files = meta_dir.list().await.map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot list metadata directory: {e:?}"))
        })?;
        files
            .filter_map(|obj| async {
                if let Ok(obj) = obj {
                    if obj.name().ends_with(".metadata.json") {
                        Some(obj.name().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<String>>()
            .await
            .into_iter()
            .max()
            .map(|s| format!("metadata/{s}"))
            .ok_or_else(|| ErrorCode::ReadTableDataError("Cannot get the latest manifest file"))
    }
}

#[async_trait]
impl Table for IcebergTable {
    fn is_local(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        todo!()
    }
}
