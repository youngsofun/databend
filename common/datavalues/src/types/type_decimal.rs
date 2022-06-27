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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::ErrorCode;
use common_exception::Result;
use rand::prelude::*;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;
use crate::serializations::DateSerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecimalType {
    precision: usize,
    scale: usize,
}

impl DecimalType {
    pub fn new_impl(precision: usize, scale: usize) -> DataTypeImpl {
        DataTypeImpl::Decimal(Self { precision, scale })
    }

    fn display(self, x: i128) -> String{
        let base = x / 10i128.pow(scale);
        let decimals = x - base * 10i128.pow(scale);
        format!("{}.{}", base, decimals)
    }
}

impl DataType for DecimalType {
    fn data_type_id(&self) -> TypeID {
        TypeID::DecimalType
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        format!("Decimal({}, {})", self.precision, self.scale)
    }

    fn default_value(&self) -> DataValue {
        DataValue::Int128(0)
    }

    fn random_value(&self) -> DataValue {
        // todo
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let value = rng.next_u64();
        DataValue::Int128(value as i128)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_i128()?;
        let column = Series::from_data(&[value as i128]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_i128())
            .collect::<Result<Vec<_>>>()?;

        let value = value.iter().map(|v| *v as i128).collect::<Vec<_>>();
        Ok(Series::from_data(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Decimal(self.precision, self.scale)
    }

    fn create_serializer_inner<'a>(&self, col: &'a ColumnRef) -> Result<TypeSerializerImpl<'a>> {
        Ok(DateSerializer::<'a, i32>::try_create(col)?.into())
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        unimplemented!()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i128>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for DecimalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
