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

pub mod ast;
pub mod expr;
pub mod rule;
pub mod token;

use std::ops::Range;

use common_exception::ErrorCode;
use common_exception::Result;
use nom::combinator::map;

use self::rule::statement::statement;
use self::token::TokenKind;
use self::token::Tokenizer;
use crate::parser::ast::Statement;
use crate::rule;

/// Parse a SQL string into `Statement`s.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let tokens = Tokenizer::new(sql).collect::<Result<Vec<_>>>()?;
    let stmt = map(rule! { #statement ~ ";" }, |(stmt, _)| stmt);
    let mut stmts = rule! { #stmt+ };
    match stmts(tokens.as_slice()) {
        Ok((rest, stmts)) if rest[0].kind == TokenKind::EOI => Ok(stmts),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(pretty_print_error(sql, vec![(
            rest[0].span.clone(),
            "unable to parse rest of the sql".to_owned(),
        )]))),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => Err(ErrorCode::SyntaxException(
            pretty_print_error(sql, err.to_labels()),
        )),
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

pub fn pretty_print_error(source: &str, lables: Vec<(Range<usize>, String)>) -> String {
    use codespan_reporting::diagnostic::Diagnostic;
    use codespan_reporting::diagnostic::Label;
    use codespan_reporting::files::SimpleFile;
    use codespan_reporting::term;
    use codespan_reporting::term::termcolor::Buffer;
    use codespan_reporting::term::Chars;
    use codespan_reporting::term::Config;

    let mut writer = Buffer::no_color();
    let file = SimpleFile::new("SQL", source);
    let config = Config {
        chars: Chars::ascii(),
        before_label_lines: 3,
        ..Default::default()
    };

    let lables = lables
        .into_iter()
        .enumerate()
        .map(|(i, (span, msg))| {
            if i == 0 {
                Label::primary((), span).with_message(msg)
            } else {
                Label::secondary((), span).with_message(msg)
            }
        })
        .collect();

    let diagnostic = Diagnostic::error().with_labels(lables);

    term::emit(&mut writer, &config, &file, &diagnostic).unwrap();

    std::str::from_utf8(&writer.into_inner())
        .unwrap()
        .to_string()
}
