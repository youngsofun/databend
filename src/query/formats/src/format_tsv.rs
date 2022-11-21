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

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::position2;
use common_io::prelude::BufferReadExt;
use common_io::prelude::FileSplit;
use common_io::prelude::FormatSettings;
use common_io::prelude::MemoryReader;
use common_io::prelude::NestedCheckpointReader;

use super::format_diagnostic::FormatDiagnostic;
use crate::format_diagnostic::verbose_string;
use crate::FormatFactory;
use crate::InputFormat;
use crate::InputState;

pub struct TsvInputState {
    pub memory: Vec<u8>,
    pub accepted_rows: usize,
    pub accepted_bytes: usize,
    pub need_more_data: bool,
    pub ignore_if_first: Option<u8>,
    pub start_row_index: usize,
    pub file_name: Option<String>,
}

impl InputState for TsvInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct TsvInputFormat {
    schema: DataSchemaRef,
    is_artificial_schema: bool,
    skip_rows: usize,
    min_accepted_rows: usize,
    min_accepted_bytes: usize,
    settings: FormatSettings,
}

impl TsvInputFormat {
    pub fn register(factory: &mut FormatFactory) {
        macro_rules! register {
            ($name: expr, $skip_rows: expr) => {
                factory.register_input(
                    $name,
                    Box::new(
                        |name: &str,
                         schema: DataSchemaRef,
                         is_artificial_schema: bool,
                         settings: FormatSettings| {
                            TsvInputFormat::try_create(
                                name,
                                schema,
                                is_artificial_schema,
                                settings,
                                $skip_rows,
                                8192,
                                10 * 1024 * 1024,
                            )
                        },
                    ),
                );
            };
        }

        register! { "Tsv", 0 }
        register! { "TsvWithNames", 1 }
        register! { "TsvWithNamesAndTypes", 2 }

        register! { "TabSeparated", 0 }
        register! { "TabSeparatedWithNames", 1 }
        register! { "TabSeparatedWithNamesAndTypes", 2 }
    }

    pub fn try_create(
        _name: &str,
        schema: DataSchemaRef,
        is_artificial_schema: bool,
        mut settings: FormatSettings,
        skip_rows: usize,
        min_accepted_rows: usize,
        min_accepted_bytes: usize,
    ) -> Result<Arc<dyn InputFormat>> {
        settings.field_delimiter = vec![b'\t'];
        settings.record_delimiter = vec![b'\n'];
        settings.null_bytes = settings.tsv_null_bytes.clone();

        Ok(Arc::new(TsvInputFormat {
            schema,
            is_artificial_schema,
            settings,
            skip_rows,
            min_accepted_rows,
            min_accepted_bytes,
        }))
    }

    fn find_delimiter(&self, buf: &[u8], pos: usize, state: &mut TsvInputState) -> usize {
        let position = pos + position2::<true, b'\r', b'\n'>(&buf[pos..]);

        if position != buf.len() {
            if buf[position] == b'\r' {
                return self.accept_row::<b'\n'>(buf, pos, state, position);
            } else if buf[position] == b'\n' {
                return self.accept_row::<0>(buf, pos, state, position);
            }
        }
        buf.len()
    }

    #[inline(always)]
    fn accept_row<const C: u8>(
        &self,
        buf: &[u8],
        pos: usize,
        state: &mut TsvInputState,
        index: usize,
    ) -> usize {
        state.accepted_rows += 1;
        state.accepted_bytes += index - pos;

        if state.accepted_rows >= self.min_accepted_rows
            || (state.accepted_bytes + index) >= self.min_accepted_bytes
        {
            state.need_more_data = false;
        }

        if C != 0 {
            if buf.len() <= index + 1 {
                state.ignore_if_first = Some(C);
            } else if buf[index + 1] == C {
                return index + 2;
            }
        }

        index + 1
    }

    fn read_row(
        &self,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        deserializers: &mut impl Iterator<Item = TypeDeserializerImpl>,
        row_index: usize,
    ) -> Result<Vec<TypeDeserializerImpl>> {
        let mut cols = vec![];
        while !checkpoint_reader.ignore_white_spaces_and_byte(b'\r')?
            && !checkpoint_reader.ignore_white_spaces_and_byte(b'\n')?
            && !checkpoint_reader.eof()?
        {
            if let Some(mut de) = deserializers.next() {
                if checkpoint_reader.ignore_white_spaces_and_byte(b'\t')? {
                    de.de_default(&self.settings);
                } else {
                    de.de_text(checkpoint_reader, &self.settings)?;
                }
                checkpoint_reader.ignore_white_spaces_and_byte(b'\t')?;
                cols.push(de);
            } else {
                return Err(ErrorCode::BadBytes(format!(
                    "Parse Tsv error at line {}",
                    row_index
                )));
            }
        }
        Ok(cols)
    }
}

impl InputFormat for TsvInputFormat {
    fn support_parallel(&self) -> bool {
        true
    }

    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(TsvInputState {
            memory: vec![],
            accepted_rows: 0,
            accepted_bytes: 0,
            need_more_data: false,
            ignore_if_first: None,
            start_row_index: 0,
            file_name: None,
        })
    }

    fn set_state(
        &self,
        state: &mut Box<dyn InputState>,
        file_name: String,
        start_row_index: usize,
    ) -> Result<()> {
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();
        state.file_name = Some(file_name);
        state.start_row_index = start_row_index;
        Ok(())
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<Vec<DataBlock>> {
        let mut state = std::mem::replace(state, self.create_state());
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();
        let memory = std::mem::take(&mut state.memory);
        self.deserialize_complete_split(FileSplit {
            path: state.file_name.clone(),
            start_offset: 0,
            start_row: state.start_row_index,
            buf: memory,
        })
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<(usize, bool)> {
        let mut index = 0;
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();

        if let Some(first) = state.ignore_if_first.take() {
            if buf[0] == first {
                index += 1;
            }
        }

        state.need_more_data = true;
        while index < buf.len() && state.need_more_data {
            index = self.find_delimiter(buf, index, state);
        }

        state.memory.extend_from_slice(&buf[0..index]);
        let finished = !state.need_more_data && state.ignore_if_first.is_none();
        Ok((index, finished))
    }

    fn take_buf(&self, state: &mut Box<dyn InputState>) -> Vec<u8> {
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();
        std::mem::take(&mut state.memory)
    }

    fn skip_header(
        &self,
        buf: &[u8],
        state: &mut Box<dyn InputState>,
        force: usize,
    ) -> Result<usize> {
        let rows_to_skip = if force > 0 { force } else { self.skip_rows };
        if rows_to_skip > 0 {
            let mut index = 0;
            let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();

            while index < buf.len() {
                index = self.find_delimiter(buf, index, state);
                if state.accepted_rows == rows_to_skip {
                    return Ok(index);
                }
            }
        }
        Ok(0)
    }

    fn read_row_num(&self, state: &mut Box<dyn InputState>) -> Result<usize> {
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();
        Ok(state.accepted_rows)
    }

    fn deserialize_complete_split(&self, split: FileSplit) -> Result<Vec<DataBlock>> {
        let memory_reader = MemoryReader::new(split.buf);
        let mut checkpoint_reader = NestedCheckpointReader::new(memory_reader);

        use common_datavalues::DataType;
        let mut deserializers = vec![];
        let mut deserializer_iter: Box<dyn Iterator<Item = TypeDeserializerImpl>> =
            if self.is_artificial_schema {
                Box::new(self.schema.fields().iter().map(|field| {
                    let data_type = field.data_type();
                    data_type.create_deserializer(self.min_accepted_rows)
                }))
            } else {
                Box::new(
                    self.schema
                        .create_deserializers(self.min_accepted_rows)
                        .into_iter(),
                )
            };

        let mut row_index = 0;
        while !checkpoint_reader.eof()? {
            checkpoint_reader.push_checkpoint();

            let res = if row_index == 0 {
                self.read_row(&mut checkpoint_reader, &mut deserializer_iter, row_index)
            } else {
                self.read_row(
                    &mut checkpoint_reader,
                    &mut deserializers.into_iter(),
                    row_index,
                )
            };
            // TODO duplicated code
            match res {
                Err(err) => {
                    let checkpoint_buffer = checkpoint_reader.get_checkpoint_buffer_end();
                    let msg = self.get_diagnostic_info(
                        checkpoint_buffer,
                        &split.path,
                        row_index + split.start_row,
                        self.schema.clone(),
                        self.min_accepted_rows,
                        self.settings.clone(),
                    )?;
                    let err = err.add_message_back(msg);
                    return Err(err);
                }
                Ok(v) => {
                    deserializers = v;
                    checkpoint_reader.pop_checkpoint();
                    row_index += 1;
                }
            }
        }

        // TODO duplicated code
        let mut columns = Vec::with_capacity(deserializers.len());
        for deserializer in &mut deserializers {
            columns.push(deserializer.finish_to_column());
        }

        let schema = if self.is_artificial_schema {
            let len = deserializers.len();
            let fields = &self.schema.fields().as_slice()[0..len];
            DataSchemaRefExt::create(fields.to_vec())
        } else {
            self.schema.clone()
        };

        Ok(vec![DataBlock::create(schema, columns)])
    }
}

#[allow(clippy::format_push_string)]
impl FormatDiagnostic for TsvInputFormat {
    fn deserialize_field(
        &self,
        deserializer: &mut TypeDeserializerImpl,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        settings: FormatSettings,
    ) -> Result<()> {
        deserializer.de_text(checkpoint_reader, &settings)
    }

    fn parse_field_delimiter_with_diagnostic_info(
        &self,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        out: &mut String,
    ) -> Result<bool> {
        let result = checkpoint_reader.must_ignore_byte(b'\t');
        if result.is_err() {
            let position = checkpoint_reader.position()?;
            if position == b'\n' {
                out.push_str("\tError: Line feed found where tab is expected.\n");
            } else if position == b'\r' {
                out.push_str("\tError: Carriage return found where tab is expected.\n");
            } else {
                out.push_str("\tError: There is no tab. ");
                verbose_string(&[position], out);
                out.push_str(" found instead.\n");
            }
            return Ok(false);
        }

        Ok(true)
    }

    fn parse_row_end_with_diagnostic_info(
        &self,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        out: &mut String,
    ) -> Result<bool> {
        checkpoint_reader.ignore_white_spaces()?;

        if checkpoint_reader.eof()? {
            return Ok(true);
        }

        let result = checkpoint_reader.must_ignore_byte(b'\n');
        if result.is_err() {
            let position = checkpoint_reader.position()?;
            if position == b'\t' {
                out.push_str("\tError: Tab found where line feed is expected.\n");
            } else if position == b'\r' {
                out.push_str("\tError: Carriage return found where line feed is expected.");
            } else {
                out.push_str("\tError: There is no line feed. ");
                verbose_string(&[position], out);
                out.push_str(" found instead.\n");
            }
            return Ok(false);
        }

        Ok(true)
    }
}
