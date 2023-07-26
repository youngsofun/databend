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

mod data_source_info;
mod parquet;
mod parquet_read_options;
mod parquet_v2;
mod result_scan;
mod stage;

pub use data_source_info::DataSourceInfo;
pub use parquet::ParquetTableInfo;
pub use parquet_read_options::ParquetReadOptions;
pub use parquet_v2::ParquetTableInfo as ParquetTableInfoV2;
pub use result_scan::ResultScanTableInfo;
pub use stage::StageTableInfo;
