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

mod copy;
mod data_mask;
pub mod expr;
#[allow(clippy::module_inception)]
mod parser;
pub mod query;
pub mod quote;
mod share;
mod stage;
pub mod statement;
pub mod token;
pub mod unescape;
pub use parser::parse_comma_separated_exprs;
pub use parser::parse_comma_separated_idents;
pub use parser::parse_expr;
pub use parser::parse_sql;
pub use parser::parser_values_with_placeholder;
pub use parser::tokenize_sql;
pub use token::all_reserved_keywords;
