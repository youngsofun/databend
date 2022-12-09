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

use common_io::prelude::*;

#[test]
fn convert_test() {
    assert_eq!(convert_byte_size(0_f64), "0.00 B");
    assert_eq!(convert_byte_size(0.1_f64), "0.10 B");
    assert_eq!(convert_byte_size(1_f64), "1.00 B");
    assert_eq!(convert_byte_size(1023_f64), "1023.00 B");
    assert_eq!(convert_byte_size(1024_f64), "1.00 KiB");
    assert_eq!(convert_byte_size(1229_f64), "1.20 KiB");
    assert_eq!(
        convert_byte_size(1024_f64 * 1024_f64 * 1024_f64),
        "1.00 GiB"
    );

    assert_eq!(convert_number_size(1_f64), "1");
    assert_eq!(convert_number_size(1022_f64), "1.02 thousand");
    assert_eq!(convert_number_size(10222_f64), "10.22 thousand");
}

#[test]
fn parse_unescape() {
    let cases = vec![
        vec!["a", "a"],
        vec!["abc", "abc"],
        vec!["\\x01", "\x01"],
        vec!["\t\nabc", "\t\nabc"],
        vec!["\"\t\nabc\"", "\"\t\nabc\""],
        vec!["\"\\t\nabc\"", "\"\t\nabc\""],
        vec!["'\\t\nabc'", "'\t\nabc'"],
        vec!["\\t\\nabc", "\t\nabc"],
    ];

    for c in cases {
        assert_eq!(unescape_string(c[0]).unwrap(), c[1]);
    }
}
