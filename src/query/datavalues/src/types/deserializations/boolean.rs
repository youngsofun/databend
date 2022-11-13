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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;

use crate::prelude::*;

pub struct BooleanDeserializer {
    pub builder: MutableBooleanColumn,
}

impl TypeDeserializer for BooleanDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.memory_size()
    }

    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let value: bool = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_value(false);
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
            let value: bool = reader.read_scalar()?;
            self.builder.append_value(value);
        }

        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Bool(v) => self.builder.append_value(*v),
            _ => return Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
        Ok(())
    }

    fn append_data_value(&mut self, value: DataValue, _format: &FormatSettings) -> Result<()> {
        self.builder.append_value(value.as_bool()?);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
