//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use opendal::io_util::CompressAlgorithm;
use common_catalog::plan::PartInfo;

pub trait DynData: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub path: String,
    pub size: usize,
    pub num_splits: usize,
    pub compress_alg: Option<CompressAlgorithm>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SplitInfo {
    pub file: Arc<FileInfo>,
    pub seq_in_file: usize,
    pub offset: usize,
    pub size: usize,
    pub num_file_splits: usize,
    pub format_info: Option<Arc<dyn DynData>>,
}

#[typetag::serde(name = "stage_file_partition")]
impl PartInfo for SplitInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<SplitInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl Debug for SplitInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitInfo")
            .field("seq_in_file", &self.seq_in_file)
            .field("offset", &self.offset)
            .field("size", &self.size)
            .finish()
    }
}

impl Display for SplitInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let n = self.file.num_splits;
        if n > 1 {
            write!(
                f,
                "{}({})[{}/{}][{}..{}]",
                self.file.path,
                self.size,
                self.seq_in_file,
                n,
                self.offset,
                self.offset + self.size
            )
        } else {
            write!(f, "{}({})", self.file.path, self.size)
        }
    }
}

pub fn split_by_size(size: usize, split_size: usize) -> Vec<(usize, usize)> {
    let mut splits = vec![];
    let n = (size + split_size - 1) / split_size;
    for i in 0..n {
        let start = i * split_size;
        let end = std::cmp::min((i + 1) * split_size, size);
        splits.push((start, end - start))
    }
    splits
}

impl SplitInfo {
    pub fn from_stream_split(path: String, compress_alg: Option<CompressAlgorithm>) -> Self {
        SplitInfo {
            file: Arc::new(FileInfo {
                path,
                size: 0,
                num_splits: 1,
                compress_alg,
            }),
            seq_in_file: 0,
            offset: 0,
            size: 0,
            num_file_splits: 1,
            format_info: None,
        }
    }
}
