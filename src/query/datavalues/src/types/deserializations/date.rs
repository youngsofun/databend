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

use std::io::Cursor;

use chrono::Datelike;
use chrono::NaiveDate;
use common_exception::*;
use common_io::cursor_ext::*;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;
use common_io::prelude::StatBuffer;
use lexical_core::FromLexical;
use micromarshal::Unmarshal;
use num::cast::AsPrimitive;

use crate::prelude::*;

pub struct DateDeserializer<T: PrimitiveType> {
    pub builder: MutablePrimitiveColumn<T>,
}

pub const EPOCH_DAYS_FROM_CE: i32 = 719_163;

impl<T> TypeDeserializer for DateDeserializer<T>
where
    i32: AsPrimitive<T>,
    T: PrimitiveType,
    T: Unmarshal<T> + StatBuffer + FromLexical,
{
    fn memory_size(&self) -> usize {
        self.builder.memory_size()
    }

    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let value: T = reader.read_scalar()?;
        check_date(value.as_i32())?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_value(T::default());
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T = reader.read_scalar()?;
            check_date(value.as_i32())?;
            self.builder.append_value(value);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::String(v) => {
                let mut reader = Cursor::new(v.as_bytes());
                let date = reader.read_date_text(&format.timezone)?;
                let days = uniform_date(date);
                check_date(days.as_i32())?;
                self.builder.append_value(days);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
    }

    fn de_whole_text(&mut self, reader: &[u8], format: &FormatSettings) -> Result<()> {
        let mut reader = Cursor::new(reader);
        let date = reader.read_date_text(&format.timezone)?;
        let days = uniform_date(date);
        check_date(days.as_i32())?;
        reader.must_eof()?;
        self.builder.append_value(days);
        Ok(())
    }

    fn de_text_quoted<R: AsRef<[u8]>>(
        &mut self,
        reader: &mut Cursor<R>,
        format: &FormatSettings,
    ) -> Result<()> {
        reader.must_ignore_byte(b'\'')?;
        let date = reader.read_date_text(&format.timezone);
        reader.must_ignore_byte(b'\'')?;
        if date.is_err() {
            return Err(date.err().unwrap());
        }
        let days = uniform_date(date.unwrap());
        check_date(days.as_i32())?;

        self.builder.append_value(days);
        Ok(())
    }

    fn de_text<R: AsRef<[u8]>>(
        &mut self,
        reader: &mut Cursor<R>,
        format: &FormatSettings,
    ) -> Result<()> {
        let date = reader.read_date_text(&format.timezone)?;
        let days = uniform_date(date);
        check_date(days.as_i32())?;
        self.builder.append_value(days);
        Ok(())
    }

    fn append_data_value(&mut self, value: DataValue, _format: &FormatSettings) -> Result<()> {
        let v = value.as_i64()? as i32;
        check_date(v)?;
        self.builder.append_value(v.as_());
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}

#[inline]
pub fn uniform_date<T>(date: NaiveDate) -> T
where
    i32: AsPrimitive<T>,
    T: PrimitiveType,
{
    (date.num_days_from_ce() - EPOCH_DAYS_FROM_CE).as_()
}
