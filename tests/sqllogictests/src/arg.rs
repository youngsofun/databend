// Copyright 2021 Datafuse Labs.
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

use clap::Parser;
// Add options when run sqllogictest, such as specific dir or file

#[derive(Parser, Debug)]
pub struct SqlLogicTestArgs {
    // Set specific dir to run
    #[arg(
        short = 'd',
        long = "dir",
        help = "Run sqllogictests in specific directory, the arg is optional"
    )]
    pub dir: Option<String>,

    // Set specific test file to run
    #[arg(
        short = 'f',
        long = "file",
        help = "Run sqllogictests in specific test file, the arg is optional"
    )]
    pub file: Option<String>,
}
