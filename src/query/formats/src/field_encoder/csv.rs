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

use common_datavalues::serializations::ArraySerializer;
use common_datavalues::serializations::StructSerializer;
use common_io::consts::FALSE_BYTES_LOWER;
use common_io::consts::INF_BYTES_LOWER;
use common_io::consts::NAN_BYTES_LOWER;
use common_io::consts::NULL_BYTES_ESCAPE;
use common_io::consts::TRUE_BYTES_LOWER;

use crate::field_encoder::FieldEncoderRowBased;
use crate::field_encoder::FieldEncoderValues;
use crate::CommonSettings;
use crate::FileFormatOptionsExt;

pub struct FieldEncoderCSV {
    pub nested: FieldEncoderValues,
    pub common_settings: CommonSettings,
    pub quote_char: u8,
}

impl FieldEncoderCSV {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldEncoderCSV {
            nested: FieldEncoderValues::create(options),
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options.timezone,
            },
            quote_char: options.quote.as_bytes()[0],
        }
    }
}

impl FieldEncoderRowBased for FieldEncoderCSV {
    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn write_string_inner(&self, in_buf: &[u8], out_buf: &mut Vec<u8>, raw: bool) {
        if raw {
            out_buf.extend_from_slice(in_buf);
        } else {
            write_csv_string(in_buf, out_buf, self.quote_char);
        }
    }

    fn write_array<'a>(
        &self,
        column: &ArraySerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let mut buf = vec![];
        self.nested.write_array(column, row_index, &mut buf, false);
        self.write_string_inner(&buf, out_buf, raw)
    }

    fn write_struct<'a>(
        &self,
        column: &StructSerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let mut buf = vec![];
        self.nested.write_struct(column, row_index, &mut buf, false);
        self.write_string_inner(&buf, out_buf, raw)
    }
}

pub fn write_csv_string(bytes: &[u8], buf: &mut Vec<u8>, quote: u8) {
    buf.push(quote);
    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if byte == quote {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            buf.push(quote);
            buf.push(quote);
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
    buf.push(quote);
}
