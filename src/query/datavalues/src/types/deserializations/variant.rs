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

use std::io::Cursor;
use std::io::Read;

use common_exception::Result;
use common_io::cursor_ext::*;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;

use crate::prelude::*;

pub struct VariantDeserializer {
    pub buffer: Vec<u8>,
    pub builder: MutableObjectColumn<VariantValue>,
    pub memory_size: usize,
}

impl VariantDeserializer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::new(),
            builder: MutableObjectColumn::<VariantValue>::with_capacity(capacity),
            memory_size: 0,
        }
    }
}

impl TypeDeserializer for VariantDeserializer {
    fn memory_size(&self) -> usize {
        self.memory_size
    }

    #[allow(clippy::uninit_vec)]
    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let offset: u64 = reader.read_uvarint()?;

        self.buffer.clear();
        self.buffer.reserve(offset as usize);
        unsafe {
            self.buffer.set_len(offset as usize);
        }

        reader.read_exact(&mut self.buffer)?;
        let val = serde_json::from_slice(self.buffer.as_slice())?;
        self.builder.append_value(val);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder
            .append_value(VariantValue::from(serde_json::Value::Null));
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let reader = &reader[step * row..];
            let val = serde_json::from_slice(reader)?;
            self.builder.append_value(val);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        let val = VariantValue::from(value);
        self.memory_size += val.calculate_memory_size();
        self.builder.append_value(val);
        Ok(())
    }

    fn de_text<R: AsRef<[u8]>>(
        &mut self,
        reader: &mut Cursor<R>,
        _format: &FormatSettings,
    ) -> Result<()> {
        self.buffer.clear();
        reader.read_escaped_string_text(&mut self.buffer)?;
        let val = serde_json::from_slice(self.buffer.as_slice())?;
        self.builder.append_value(val);
        self.memory_size += self.buffer.len();
        Ok(())
    }

    fn de_text_quoted<R: AsRef<[u8]>>(
        &mut self,
        reader: &mut Cursor<R>,
        _format: &FormatSettings,
    ) -> Result<()> {
        self.buffer.clear();
        reader.read_quoted_text(&mut self.buffer, b'\'')?;

        let val = serde_json::from_slice(self.buffer.as_slice())?;

        self.builder.append_value(val);
        self.memory_size += self.buffer.len();
        Ok(())
    }

    fn append_data_value(&mut self, value: DataValue, _format: &FormatSettings) -> Result<()> {
        self.builder.append_data_value(value)
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
