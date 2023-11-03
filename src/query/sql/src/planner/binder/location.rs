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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::storage::StorageParams;
use common_meta_app::storage::StorageS3Config;

/// parse_uri_location will parse given UriLocation into StorageParams and Path.
pub async fn bind_uri_location(sp: &StorageParams) -> Result<StorageParams> {
    #[cfg(not(feature = "debug"))]
    if matches!(sp, StorageParams::Hdfs(..)) {
        return Err(ErrorCode::BadArguments(
            "HDFS is not supported yet".to_string(),
        ));
    }
    let sp = match sp {
        StorageParams::S3(s3) if s3.region.is_empty() => {
            let region = opendal::services::S3::detect_region(&s3.endpoint_url, &s3.bucket)
                .await
                .unwrap_or_default();
            StorageParams::S3(StorageS3Config {
                region,
                ..s3.clone()
            })
        }
        v => v.clone(),
    };
    let sp = sp.auto_detect().await;

    Ok(sp)
}
