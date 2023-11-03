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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use common_exception::ErrorCode;

/// Storage Services that Databend supports
///
/// # Notes
///
/// - This is a subset of opendal::Scheme.
/// - New variant SHOULD be added in alphabet orders,
/// - Users MUST NOT relay on its order.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Scheme {
    /// [azblob][crate::services::Azblob]: Azure Storage Blob services.
    Azblob,
    /// [fs][crate::services::Fs]: POSIX alike file system.
    Fs,
    /// [gcs][crate::services::Gcs]: Google Cloud Storage backend.
    Gcs,
    /// [ghac][crate::services::Ghac]: GitHub Action Cache services.
    Hdfs,
    /// [http][crate::services::Http]: HTTP backend.
    Http,
    /// [ipmfs][crate::services::Ipfs]: IPFS HTTP Gateway
    Ipfs,
    /// [memory][crate::services::Memory]: In memory backend support.
    Memory,
    /// [obs][crate::services::Obs]: Huawei Cloud OBS services.
    Obs,
    /// [oss][crate::services::Oss]: Aliyun Object Storage Services
    Oss,
    /// [s3][crate::services::S3]: AWS S3 alike services.
    S3,
    /// [webhdfs][crate::services::Webhdfs]: WebHDFS RESTful API Services
    Webhdfs,
}

impl Scheme {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Default for Scheme {
    fn default() -> Self {
        Self::Memory
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl FromStr for Scheme {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "gcs" => Ok(Scheme::Gcs),
            "hdfs" => Ok(Scheme::Hdfs),
            "http" | "https" => Ok(Scheme::Http),
            "ipfs" | "ipns" => Ok(Scheme::Ipfs),
            "memory" => Ok(Scheme::Memory),
            "obs" => Ok(Scheme::Obs),
            "s3" => Ok(Scheme::S3),
            "oss" => Ok(Scheme::Oss),
            "webhdfs" => Ok(Scheme::Webhdfs),
            _ => Err(ErrorCode::StorageUnsupported(format!(
                "Unknown scheme: {:?}",
                s
            ))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Azblob => "azblob",
            Scheme::Fs => "fs",
            Scheme::Gcs => "gcs",
            Scheme::Hdfs => "hdfs",
            Scheme::Http => "http",
            Scheme::Ipfs => "ipfs",
            Scheme::Memory => "memory",
            Scheme::Obs => "obs",
            Scheme::Oss => "oss",
            Scheme::S3 => "s3",
            Scheme::Webhdfs => "webhdfs",
        }
    }
}

impl From<Scheme> for String {
    fn from(v: Scheme) -> Self {
        v.into_static().to_string()
    }
}
