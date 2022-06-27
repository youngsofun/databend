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
use common_macros::MallocSizeOf;

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    MallocSizeOf,
)]
pub enum TypeID {
    Null,
    Nullable,
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,

    Decimal,
    String,

    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits), it's physical type is Int32
    Date,

    /// A 64-bit timestamp representing the elapsed time since UNIX epoch (1970-01-01)
    /// in microseconds, it's physical type is Int64
    /// store UTC timestamp
    Timestamp,

    /// Interval represents the time interval, e.g. the elapsed time between two date or timestamp.
    /// Underneath Interval is stored as int64, so it supports negative values.
    Interval,

    Array,
    Struct,

    /// Variant is a tagged universal type, which can store values of any other type,
    /// including Object and Array, up to a maximum size of 16 MB.
    Variant,
    VariantArray,
    VariantObject,
}

impl TypeID {
    #[inline]
    pub fn null_at(&self) -> bool {
        matches!(self, TypeID::Nullable)
    }

    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, TypeID::String)
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, TypeID::Null)
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            TypeID::Int8
                | TypeID::Int16
                | TypeID::Int32
                | TypeID::Int64
                | TypeID::UInt8
                | TypeID::UInt16
                | TypeID::UInt32
                | TypeID::UInt64
        )
    }

    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        matches!(
            self,
            TypeID::Int8 | TypeID::Int16 | TypeID::Int32 | TypeID::Int64
        )
    }

    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
            TypeID::UInt8 | TypeID::UInt16 | TypeID::UInt32 | TypeID::UInt64
        )
    }

    #[inline]
    pub fn is_floating(&self) -> bool {
        matches!(self, TypeID::Float32 | TypeID::Float64)
    }

    #[inline]
    pub fn is_date_or_date_time(&self) -> bool {
        matches!(self, TypeID::Date | TypeID::Timestamp,)
    }

    /// Determine if a TypeID is signed numeric or not
    #[inline]
    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            TypeID::Int8
                | TypeID::Int16
                | TypeID::Int32
                | TypeID::Int64
                | TypeID::Float32
                | TypeID::Float64
        )
    }

    /// Determine if a TypeID is numeric or not
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_floating()
    }

    #[inline]
    pub fn is_interval(&self) -> bool {
        matches!(self, TypeID::Interval)
    }

    #[inline]
    pub fn is_array(&self) -> bool {
        matches!(self, TypeID::Array)
    }

    #[inline]
    pub fn is_struct(&self) -> bool {
        matches!(self, TypeID::Struct)
    }

    #[inline]
    pub fn is_quoted(&self) -> bool {
        matches!(self, TypeID::String | TypeID::Date | TypeID::Timestamp)
    }

    #[inline]
    pub fn is_variant(&self) -> bool {
        matches!(
            self,
            TypeID::Variant | TypeID::VariantArray | TypeID::VariantObject
        )
    }

    #[inline]
    pub fn is_variant_or_array(&self) -> bool {
        matches!(self, TypeID::Variant | TypeID::VariantArray)
    }

    #[inline]
    pub fn is_variant_or_object(&self) -> bool {
        matches!(self, TypeID::Variant | TypeID::VariantObject)
    }

    #[inline]
    pub fn numeric_byte_size(&self) -> Result<usize> {
        match self {
            TypeID::Int8 | TypeID::UInt8 => Ok(1),
            TypeID::Int16 | TypeID::UInt16 => Ok(2),
            TypeID::Int32 | TypeID::UInt32 | TypeID::Float32 | TypeID::Date => Ok(4),
            TypeID::Int64 | TypeID::UInt64 | TypeID::Float64 | TypeID::Timestamp => Ok(8),
            _ => Result::Err(ErrorCode::BadArguments(format!(
                "Function number_byte_size argument must be numeric types, but got {:?}",
                self
            ))),
        }
    }

    pub fn to_physical_type(&self) -> PhysicalTypeID {
        use TypeID::*;
        match self {
            Nullable => PhysicalTypeID::Nullable,
            Null => PhysicalTypeID::Null,
            Boolean => PhysicalTypeID::Boolean,
            Int8 => PhysicalTypeID::Int8,
            Int16 => PhysicalTypeID::Int16,

            Int32 | Date => PhysicalTypeID::Int32,
            Int64 | Interval | Timestamp => PhysicalTypeID::Int64,

            UInt8 => PhysicalTypeID::UInt8,
            UInt16 => PhysicalTypeID::UInt16,
            UInt32 => PhysicalTypeID::UInt32,
            UInt64 => PhysicalTypeID::UInt64,
            Float32 => PhysicalTypeID::Float32,
            Float64 => PhysicalTypeID::Float64,

            String => PhysicalTypeID::String,
            Array => PhysicalTypeID::Array,
            Struct => PhysicalTypeID::Struct,
            Variant | VariantArray | VariantObject => PhysicalTypeID::Variant,
        }
    }
}

impl std::fmt::Display for TypeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeID::VariantArray => write!(f, "Array"),
            TypeID::VariantObject => write!(f, "Object"),
            _ => write!(f, "{:?}", self),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhysicalTypeID {
    Null,
    Nullable,
    Boolean,
    String,

    Array,
    Struct,
    Variant,

    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
}
