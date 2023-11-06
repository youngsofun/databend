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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use itertools::Itertools;

use crate::ast::write_comma_separated_map;
use crate::ast::write_comma_separated_quoted_list;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::UriLocation;

#[derive(Debug, Clone, PartialEq)]
pub struct TableIdentifier {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl TableIdentifier {
    pub fn from_tuple(t: (Option<Identifier>, Option<Identifier>, Identifier)) -> Self {
        let (catalog, database, table) = t;
        Self {
            catalog,
            database,
            table,
        }
    }
}

impl Display for TableIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(catalog) = &self.catalog {
            write!(
                f,
                "{catalog}.{}.{}",
                self.database.as_ref().expect("database must be valid"),
                self.table
            )?;
        } else if let Some(database) = &self.database {
            write!(f, "{database}.{}", self.table)?;
        } else {
            write!(f, "{}", self.table)?;
        };
        Ok(())
    }
}

/// CopyIntoTableStmt is the parsed statement of `COPY into <table> from <location>`.
///
/// ## Examples
///
/// ```sql
/// COPY INTO table from s3://bucket/path/to/x.csv
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CopyIntoTableStmt {
    pub src: CopyIntoTableSource,
    pub dst: TableIdentifier,
    pub dst_columns: Option<Vec<Identifier>>,

    pub hints: Option<Hint>,

    pub file_format: BTreeMap<String, String>,

    // files to load
    pub files: Option<Vec<String>>,
    pub pattern: Option<String>,
    pub force: bool,

    // copy options
    /// TODO(xuanwo): parse into validation_mode directly.
    pub validation_mode: String,
    pub size_limit: usize,
    pub max_files: usize,
    pub split_size: usize,
    pub purge: bool,
    pub disable_variant_check: bool,
    pub return_failed_only: bool,
    pub on_error: String,
}

impl CopyIntoTableStmt {
    pub fn apply_option(&mut self, opt: CopyIntoTableOption) {
        match opt {
            CopyIntoTableOption::Files(v) => self.files = Some(v),
            CopyIntoTableOption::Pattern(v) => self.pattern = Some(v),
            CopyIntoTableOption::FileFormat(v) => self.file_format = v,
            CopyIntoTableOption::ValidationMode(v) => self.validation_mode = v,
            CopyIntoTableOption::SizeLimit(v) => self.size_limit = v,
            CopyIntoTableOption::MaxFiles(v) => self.max_files = v,
            CopyIntoTableOption::SplitSize(v) => self.split_size = v,
            CopyIntoTableOption::Purge(v) => self.purge = v,
            CopyIntoTableOption::Force(v) => self.force = v,
            CopyIntoTableOption::DisableVariantCheck(v) => self.disable_variant_check = v,
            CopyIntoTableOption::ReturnFailedOnly(v) => self.return_failed_only = v,
            CopyIntoTableOption::OnError(v) => self.on_error = v,
        }
    }
}

impl Display for CopyIntoTableStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COPY")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write!(f, " INTO {}", self.dst)?;
        if let Some(columns) = &self.dst_columns {
            write!(f, "({})", columns.iter().map(|c| c.to_string()).join(","))?;
        }
        write!(f, " FROM {}", self.src)?;

        if let Some(files) = &self.files {
            write!(f, " FILES = (")?;
            write_comma_separated_quoted_list(f, files)?;
            write!(f, " )")?;
        }

        if let Some(pattern) = &self.pattern {
            write!(f, " PATTERN = '{}'", pattern)?;
        }

        if !self.file_format.is_empty() {
            write!(f, " FILE_FORMAT = (")?;
            write_comma_separated_map(f, &self.file_format)?;
            write!(f, ")")?;
        }

        if !self.validation_mode.is_empty() {
            write!(f, "VALIDATION_MODE = {}", self.validation_mode)?;
        }

        if self.size_limit != 0 {
            write!(f, " SIZE_LIMIT = {}", self.size_limit)?;
        }

        if self.max_files != 0 {
            write!(f, " MAX_FILES = {}", self.max_files)?;
        }

        if self.split_size != 0 {
            write!(f, " SPLIT_SIZE = {}", self.split_size)?;
        }

        write!(f, " PURGE = {}", self.purge)?;
        write!(f, " FORCE = {}", self.force)?;
        write!(f, " DISABLE_VARIANT_CHECK = {}", self.disable_variant_check)?;
        write!(f, " ON_ERROR = '{}'", self.on_error)?;

        Ok(())
    }
}

/// CopyIntoLocationStmt is the parsed statement of `COPY into <location>  from <table> ...`
#[derive(Debug, Clone, PartialEq)]
pub struct CopyIntoLocationStmt {
    pub hints: Option<Hint>,
    pub src: CopyIntoLocationSource,
    pub dst: FileLocation,
    pub file_format: BTreeMap<String, String>,
    pub single: bool,
    pub max_file_size: usize,
}

impl Display for CopyIntoLocationStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COPY")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write!(f, " INTO {}", self.dst)?;
        write!(f, " FROM {}", self.src)?;

        if !self.file_format.is_empty() {
            write!(f, " FILE_FORMAT = (")?;
            write_comma_separated_map(f, &self.file_format)?;
            write!(f, ")")?;
        }
        write!(f, " SINGLE = {}", self.single)?;
        write!(f, " MAX_FILE_SIZE= {}", self.max_file_size)?;

        Ok(())
    }
}

impl CopyIntoLocationStmt {
    pub fn apply_option(&mut self, opt: CopyIntoLocationOption) {
        match opt {
            CopyIntoLocationOption::FileFormat(v) => self.file_format = v,
            CopyIntoLocationOption::Single(v) => self.single = v,
            CopyIntoLocationOption::MaxFileSize(v) => self.max_file_size = v,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyIntoTableSource {
    Location(FileLocation),
    /// Load with Transform
    /// limited to `(SELECT ... FROM <location>)`
    Query(Box<Query>),
}

impl Display for CopyIntoTableSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyIntoTableSource::Location(v) => v.fmt(f),
            CopyIntoTableSource::Query(query) => {
                write!(f, "({query})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyIntoLocationSource {
    Query(Box<Query>),
    /// it will be rewrite as `(SELECT * FROM table)`
    Table(TableIdentifier),
}

impl Display for CopyIntoLocationSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyIntoLocationSource::Query(query) => {
                write!(f, "({query})")
            }
            CopyIntoLocationSource::Table(table) => {
                write!(f, "{}", table)
            }
        }
    }
}

/// StageLocation (a.k.a internal and external stage) can be used
/// in `INTO` or `FROM`.
///
/// For examples:
///
/// - internal stage: `@internal_stage/path/to/dir/`
/// - external stage: `@s3_external_stage/path/to/dir/`
/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileLocation {
    Stage(String),
    Uri(UriLocation),
}

pub enum FileLocationNoConnection {
    Stage(String),
    Uri(String),
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileLocation::Uri(loc) => {
                write!(f, "{}", loc)
            }
            FileLocation::Stage(loc) => {
                write!(f, "@{}", loc)
            }
        }
    }
}

pub enum CopyIntoTableOption {
    Files(Vec<String>),
    Pattern(String),
    FileFormat(BTreeMap<String, String>),
    ValidationMode(String),
    SizeLimit(usize),
    MaxFiles(usize),
    SplitSize(usize),
    Purge(bool),
    Force(bool),
    DisableVariantCheck(bool),
    ReturnFailedOnly(bool),
    OnError(String),
}

pub enum CopyIntoLocationOption {
    FileFormat(BTreeMap<String, String>),
    MaxFileSize(usize),
    Single(bool),
}
