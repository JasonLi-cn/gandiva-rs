/*
 * Copyright 2024 JasonLi-cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};

use crate::error::gandiva_error::GandivaResult;

pub struct ArrowTypeHelper {}

impl ArrowTypeHelper {
    pub fn arrow_field_to_protobuf(field: &Field) -> GandivaResult<crate::proto::Field> {
        Ok(crate::proto::Field {
            name: Some(field.name().clone()),
            r#type: Some(Self::arrow_type_to_protobuf(field.data_type().clone())?),
            nullable: Some(field.is_nullable()),
            children: vec![],
        })
    }

    pub fn arrow_type_to_protobuf(
        arrow_type: DataType,
    ) -> GandivaResult<crate::proto::ExtGandivaType> {
        use crate::proto::GandivaType;
        let mut ext_type: crate::proto::ExtGandivaType = crate::proto::ExtGandivaType::default();

        let ty = match arrow_type {
            DataType::Null => GandivaType::None,
            DataType::Boolean => GandivaType::Bool,
            DataType::UInt8 => GandivaType::Uint8,
            DataType::Int8 => GandivaType::Int8,
            DataType::UInt16 => GandivaType::Uint16,
            DataType::Int16 => GandivaType::Int16,
            DataType::UInt32 => GandivaType::Uint32,
            DataType::Int32 => GandivaType::Int32,
            DataType::UInt64 => GandivaType::Uint64,
            DataType::Int64 => GandivaType::Int64,
            DataType::Float16 => GandivaType::HalfFloat,
            DataType::Float32 => GandivaType::Float,
            DataType::Float64 => GandivaType::Double,
            DataType::Utf8 => GandivaType::Utf8,
            DataType::Binary => GandivaType::Binary,
            DataType::FixedSizeBinary(_) => GandivaType::FixedSizeBinary,
            DataType::Date32 => GandivaType::Date32,
            DataType::Date64 => GandivaType::Date64,
            DataType::Timestamp(time_unit, _) => {
                let time_unit = Self::time_unit_to_protobuf(time_unit);
                ext_type.time_unit = Some(time_unit as i32);
                GandivaType::Timestamp
            }
            DataType::Time32(time_unit) => {
                let time_unit = Self::time_unit_to_protobuf(time_unit);
                ext_type.time_unit = Some(time_unit as i32);
                GandivaType::Time32
            }
            DataType::Time64(time_unit) => {
                let time_unit = Self::time_unit_to_protobuf(time_unit);
                ext_type.time_unit = Some(time_unit as i32);
                GandivaType::Time64
            }
            DataType::Interval(interval_unit) => {
                let interval_unit = match interval_unit {
                    IntervalUnit::YearMonth => crate::proto::IntervalType::YearMonth,
                    IntervalUnit::DayTime => crate::proto::IntervalType::DayTime,
                    IntervalUnit::MonthDayNano => unimplemented!(),
                };
                ext_type.interval_type = Some(interval_unit as i32);
                GandivaType::Interval
            }
            DataType::Decimal128(precision, scale) => {
                ext_type.precision = Some(precision as i32);
                ext_type.scale = Some(scale as i32);
                GandivaType::Decimal
            }
            DataType::Decimal256(precision, scale) => {
                ext_type.precision = Some(precision as i32);
                ext_type.scale = Some(scale as i32);
                GandivaType::Decimal
            }
            // DataType::List(_) => GandivaType::List,
            // DataType::Struct(_) => GandivaType::Struct,
            // DataType::Union(_, _) => GandivaType::Union,
            // DataType::Dictionary(_, _) => GandivaType::Dictionary,
            // DataType::Map(_, _) => GandivaType::Map,
            other => unimplemented!("{}", other),
        } as i32;

        ext_type.r#type = Some(ty);

        Ok(ext_type)
    }

    fn time_unit_to_protobuf(time_unit: TimeUnit) -> crate::proto::TimeUnit {
        match time_unit {
            TimeUnit::Second => crate::proto::TimeUnit::Sec,
            TimeUnit::Millisecond => crate::proto::TimeUnit::Millisec,
            TimeUnit::Microsecond => crate::proto::TimeUnit::Microsec,
            TimeUnit::Nanosecond => crate::proto::TimeUnit::Nanosec,
        }
    }

    pub fn arrow_schema_to_protobuf(schema: &Schema) -> GandivaResult<crate::proto::Schema> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| Self::arrow_field_to_protobuf(field))
            .collect::<GandivaResult<Vec<_>>>()?;
        Ok(crate::proto::Schema { columns })
    }
}
