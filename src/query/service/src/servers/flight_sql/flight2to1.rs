// Copyright 2023 Datafuse Labs.
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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

use arrow_flight::FlightData;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::flight::WriteOptions;
use common_arrow::arrow::io::ipc::read::deserialize_schema;
use common_arrow::arrow::io::ipc::IpcField;
use common_arrow::arrow_format::flight::data::FlightData as FlightData2;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;

fn flight_data_1to2(data: FlightData) -> FlightData2 {
    FlightData2 {
        flight_descriptor: None,
        data_header: data.data_header.into(),
        app_metadata: data.app_metadata.into(),
        data_body: data.data_body.into(),
    }
}

fn flight_data_2to1(data: FlightData2) -> FlightData {
    FlightData {
        flight_descriptor: None,
        data_header: data.data_header.into(),
        app_metadata: data.app_metadata.into(),
        data_body: data.data_body.into(),
    }
}

pub fn data_block_to_flight_data(
    data_block: DataBlock,
    ipc_field: &[IpcField],
    options: &WriteOptions,
) -> Result<FlightData> {
    let flight_data1 = if data_block.is_empty() {
        serialize_batch(&Chunk::new(vec![]), &[], options)?.1
    } else {
        let chunks = data_block.try_into()?;
        let (dicts, flight_data1) = serialize_batch(&chunks, ipc_field, options)?;
        if !dicts.is_empty() {
            return Err(ErrorCode::Unimplemented(
                "DatabendQuery does not implement dicts.",
            ));
        }
        flight_data1
    };
    Ok(flight_data_2to1(flight_data1))
}

pub fn flight_datas_to_data_block(datas: Vec<FlightData>) -> Result<(DataSchema, DataBlock)> {
    let datas = datas.into_iter().map(flight_data_1to2).collect::<Vec<_>>();
    let (schema2, ipc_schema) = deserialize_schema(&datas[0].data_header)?;
    let chunks = deserialize_batch(&datas[1], &schema2.fields, &ipc_schema, &Default::default())?;
    let fields = schema2.fields.iter().map(|f| f.into()).collect::<Vec<_>>();
    let schema = DataSchema::new(fields);
    let data_block = DataBlock::from_arrow_chunk(&chunks, &schema)?;
    Ok((schema, data_block))
}
