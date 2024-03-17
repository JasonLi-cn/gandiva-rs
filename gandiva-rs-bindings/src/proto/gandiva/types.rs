#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtGandivaType {
    #[prost(enumeration = "GandivaType", optional, tag = "1")]
    pub r#type: ::core::option::Option<i32>,
    /// used by FIXED_SIZE_BINARY
    #[prost(uint32, optional, tag = "2")]
    pub width: ::core::option::Option<u32>,
    /// used by DECIMAL
    #[prost(int32, optional, tag = "3")]
    pub precision: ::core::option::Option<i32>,
    /// used by DECIMAL
    #[prost(int32, optional, tag = "4")]
    pub scale: ::core::option::Option<i32>,
    /// used by DATE32/DATE64
    #[prost(enumeration = "DateUnit", optional, tag = "5")]
    pub date_unit: ::core::option::Option<i32>,
    /// used by TIME32/TIME64
    #[prost(enumeration = "TimeUnit", optional, tag = "6")]
    pub time_unit: ::core::option::Option<i32>,
    /// used by TIMESTAMP
    #[prost(string, optional, tag = "7")]
    pub time_zone: ::core::option::Option<::prost::alloc::string::String>,
    /// used by INTERVAL
    #[prost(enumeration = "IntervalType", optional, tag = "8")]
    pub interval_type: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    /// name of the field
    #[prost(string, optional, tag = "1")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::core::option::Option<ExtGandivaType>,
    #[prost(bool, optional, tag = "3")]
    pub nullable: ::core::option::Option<bool>,
    /// for complex data types like structs, unions
    #[prost(message, repeated, tag = "4")]
    pub children: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GandivaDataTypes {
    #[prost(message, repeated, tag = "1")]
    pub data_type: ::prost::alloc::vec::Vec<ExtGandivaType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GandivaFunctions {
    #[prost(message, repeated, tag = "1")]
    pub function: ::prost::alloc::vec::Vec<FunctionSignature>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionSignature {
    #[prost(string, optional, tag = "1")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub return_type: ::core::option::Option<ExtGandivaType>,
    #[prost(message, repeated, tag = "3")]
    pub param_types: ::prost::alloc::vec::Vec<ExtGandivaType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldNode {
    #[prost(message, optional, tag = "1")]
    pub field: ::core::option::Option<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionNode {
    #[prost(string, optional, tag = "1")]
    pub function_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "2")]
    pub in_args: ::prost::alloc::vec::Vec<TreeNode>,
    #[prost(message, optional, tag = "3")]
    pub return_type: ::core::option::Option<ExtGandivaType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IfNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub cond: ::core::option::Option<::prost::alloc::boxed::Box<TreeNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub then_node: ::core::option::Option<::prost::alloc::boxed::Box<TreeNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub else_node: ::core::option::Option<::prost::alloc::boxed::Box<TreeNode>>,
    #[prost(message, optional, tag = "4")]
    pub return_type: ::core::option::Option<ExtGandivaType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AndNode {
    #[prost(message, repeated, tag = "1")]
    pub args: ::prost::alloc::vec::Vec<TreeNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OrNode {
    #[prost(message, repeated, tag = "1")]
    pub args: ::prost::alloc::vec::Vec<TreeNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NullNode {
    #[prost(message, optional, tag = "1")]
    pub r#type: ::core::option::Option<ExtGandivaType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BooleanNode {
    #[prost(bool, optional, tag = "1")]
    pub value: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UInt8Node {
    #[prost(uint32, optional, tag = "1")]
    pub value: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UInt16Node {
    #[prost(uint32, optional, tag = "1")]
    pub value: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UInt32Node {
    #[prost(uint32, optional, tag = "1")]
    pub value: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UInt64Node {
    #[prost(uint64, optional, tag = "1")]
    pub value: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int8Node {
    #[prost(int32, optional, tag = "1")]
    pub value: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int16Node {
    #[prost(int32, optional, tag = "1")]
    pub value: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int32Node {
    #[prost(int32, optional, tag = "1")]
    pub value: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int64Node {
    #[prost(int64, optional, tag = "1")]
    pub value: ::core::option::Option<i64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Float32Node {
    #[prost(float, optional, tag = "1")]
    pub value: ::core::option::Option<f32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Float64Node {
    #[prost(double, optional, tag = "1")]
    pub value: ::core::option::Option<f64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringNode {
    #[prost(bytes = "vec", optional, tag = "1")]
    pub value: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryNode {
    #[prost(bytes = "vec", optional, tag = "1")]
    pub value: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecimalNode {
    #[prost(string, optional, tag = "1")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int32, optional, tag = "2")]
    pub precision: ::core::option::Option<i32>,
    #[prost(int32, optional, tag = "3")]
    pub scale: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub node: ::core::option::Option<::prost::alloc::boxed::Box<TreeNode>>,
    #[prost(message, optional, tag = "2")]
    pub int32_values: ::core::option::Option<Int32Constants>,
    #[prost(message, optional, tag = "3")]
    pub int64_values: ::core::option::Option<Int64Constants>,
    #[prost(message, optional, tag = "4")]
    pub decimal_values: ::core::option::Option<DecimalConstants>,
    #[prost(message, optional, tag = "5")]
    pub string_values: ::core::option::Option<StringConstants>,
    #[prost(message, optional, tag = "6")]
    pub binary_values: ::core::option::Option<BinaryConstants>,
    #[prost(message, optional, tag = "7")]
    pub float32_values: ::core::option::Option<Float32Constants>,
    #[prost(message, optional, tag = "8")]
    pub float64_values: ::core::option::Option<Float64Constants>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int32Constants {
    #[prost(message, repeated, tag = "1")]
    pub int32_values: ::prost::alloc::vec::Vec<Int32Node>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int64Constants {
    #[prost(message, repeated, tag = "1")]
    pub int64_values: ::prost::alloc::vec::Vec<Int64Node>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecimalConstants {
    #[prost(message, repeated, tag = "1")]
    pub decimal_values: ::prost::alloc::vec::Vec<DecimalNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringConstants {
    #[prost(message, repeated, tag = "1")]
    pub string_values: ::prost::alloc::vec::Vec<StringNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryConstants {
    #[prost(message, repeated, tag = "1")]
    pub binary_values: ::prost::alloc::vec::Vec<BinaryNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Float32Constants {
    #[prost(message, repeated, tag = "1")]
    pub float32_values: ::prost::alloc::vec::Vec<Float32Node>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Float64Constants {
    #[prost(message, repeated, tag = "1")]
    pub float64_values: ::prost::alloc::vec::Vec<Float64Node>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TreeNode {
    #[prost(message, optional, tag = "1")]
    pub field_node: ::core::option::Option<FieldNode>,
    #[prost(message, optional, tag = "2")]
    pub fn_node: ::core::option::Option<FunctionNode>,
    /// control expressions
    #[prost(message, optional, boxed, tag = "6")]
    pub if_node: ::core::option::Option<::prost::alloc::boxed::Box<IfNode>>,
    #[prost(message, optional, tag = "7")]
    pub and_node: ::core::option::Option<AndNode>,
    #[prost(message, optional, tag = "8")]
    pub or_node: ::core::option::Option<OrNode>,
    /// literals
    #[prost(message, optional, tag = "11")]
    pub null_node: ::core::option::Option<NullNode>,
    #[prost(message, optional, tag = "12")]
    pub boolean_node: ::core::option::Option<BooleanNode>,
    #[prost(message, optional, tag = "13")]
    pub uint8_node: ::core::option::Option<UInt8Node>,
    #[prost(message, optional, tag = "14")]
    pub uint16_node: ::core::option::Option<UInt16Node>,
    #[prost(message, optional, tag = "15")]
    pub uint32_node: ::core::option::Option<UInt32Node>,
    #[prost(message, optional, tag = "16")]
    pub uint64_node: ::core::option::Option<UInt64Node>,
    #[prost(message, optional, tag = "17")]
    pub int8_node: ::core::option::Option<Int8Node>,
    #[prost(message, optional, tag = "18")]
    pub int16_node: ::core::option::Option<Int16Node>,
    #[prost(message, optional, tag = "19")]
    pub int32_node: ::core::option::Option<Int32Node>,
    #[prost(message, optional, tag = "20")]
    pub int64_node: ::core::option::Option<Int64Node>,
    #[prost(message, optional, tag = "21")]
    pub float32_node: ::core::option::Option<Float32Node>,
    #[prost(message, optional, tag = "22")]
    pub float64_node: ::core::option::Option<Float64Node>,
    #[prost(message, optional, tag = "23")]
    pub string_node: ::core::option::Option<StringNode>,
    #[prost(message, optional, tag = "24")]
    pub binary_node: ::core::option::Option<BinaryNode>,
    #[prost(message, optional, tag = "25")]
    pub decimal_node: ::core::option::Option<DecimalNode>,
    /// in expr
    #[prost(message, optional, boxed, tag = "26")]
    pub in_node: ::core::option::Option<::prost::alloc::boxed::Box<InNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExpressionRoot {
    #[prost(message, optional, tag = "1")]
    pub root: ::core::option::Option<TreeNode>,
    #[prost(message, optional, tag = "2")]
    pub result_type: ::core::option::Option<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExpressionList {
    #[prost(message, repeated, tag = "2")]
    pub exprs: ::prost::alloc::vec::Vec<ExpressionRoot>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Condition {
    #[prost(message, optional, tag = "1")]
    pub root: ::core::option::Option<TreeNode>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum GandivaType {
    /// arrow::Type::NA
    None = 0,
    /// arrow::Type::BOOL
    Bool = 1,
    /// arrow::Type::UINT8
    Uint8 = 2,
    /// arrow::Type::INT8
    Int8 = 3,
    /// represents arrow::Type fields in src/arrow/type.h
    Uint16 = 4,
    Int16 = 5,
    Uint32 = 6,
    Int32 = 7,
    Uint64 = 8,
    Int64 = 9,
    HalfFloat = 10,
    Float = 11,
    Double = 12,
    Utf8 = 13,
    Binary = 14,
    FixedSizeBinary = 15,
    Date32 = 16,
    Date64 = 17,
    Timestamp = 18,
    Time32 = 19,
    Time64 = 20,
    Interval = 21,
    Decimal = 22,
    List = 23,
    Struct = 24,
    Union = 25,
    Dictionary = 26,
    Map = 27,
}
impl GandivaType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            GandivaType::None => "NONE",
            GandivaType::Bool => "BOOL",
            GandivaType::Uint8 => "UINT8",
            GandivaType::Int8 => "INT8",
            GandivaType::Uint16 => "UINT16",
            GandivaType::Int16 => "INT16",
            GandivaType::Uint32 => "UINT32",
            GandivaType::Int32 => "INT32",
            GandivaType::Uint64 => "UINT64",
            GandivaType::Int64 => "INT64",
            GandivaType::HalfFloat => "HALF_FLOAT",
            GandivaType::Float => "FLOAT",
            GandivaType::Double => "DOUBLE",
            GandivaType::Utf8 => "UTF8",
            GandivaType::Binary => "BINARY",
            GandivaType::FixedSizeBinary => "FIXED_SIZE_BINARY",
            GandivaType::Date32 => "DATE32",
            GandivaType::Date64 => "DATE64",
            GandivaType::Timestamp => "TIMESTAMP",
            GandivaType::Time32 => "TIME32",
            GandivaType::Time64 => "TIME64",
            GandivaType::Interval => "INTERVAL",
            GandivaType::Decimal => "DECIMAL",
            GandivaType::List => "LIST",
            GandivaType::Struct => "STRUCT",
            GandivaType::Union => "UNION",
            GandivaType::Dictionary => "DICTIONARY",
            GandivaType::Map => "MAP",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NONE" => Some(Self::None),
            "BOOL" => Some(Self::Bool),
            "UINT8" => Some(Self::Uint8),
            "INT8" => Some(Self::Int8),
            "UINT16" => Some(Self::Uint16),
            "INT16" => Some(Self::Int16),
            "UINT32" => Some(Self::Uint32),
            "INT32" => Some(Self::Int32),
            "UINT64" => Some(Self::Uint64),
            "INT64" => Some(Self::Int64),
            "HALF_FLOAT" => Some(Self::HalfFloat),
            "FLOAT" => Some(Self::Float),
            "DOUBLE" => Some(Self::Double),
            "UTF8" => Some(Self::Utf8),
            "BINARY" => Some(Self::Binary),
            "FIXED_SIZE_BINARY" => Some(Self::FixedSizeBinary),
            "DATE32" => Some(Self::Date32),
            "DATE64" => Some(Self::Date64),
            "TIMESTAMP" => Some(Self::Timestamp),
            "TIME32" => Some(Self::Time32),
            "TIME64" => Some(Self::Time64),
            "INTERVAL" => Some(Self::Interval),
            "DECIMAL" => Some(Self::Decimal),
            "LIST" => Some(Self::List),
            "STRUCT" => Some(Self::Struct),
            "UNION" => Some(Self::Union),
            "DICTIONARY" => Some(Self::Dictionary),
            "MAP" => Some(Self::Map),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DateUnit {
    Day = 0,
    Milli = 1,
}
impl DateUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DateUnit::Day => "DAY",
            DateUnit::Milli => "MILLI",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "DAY" => Some(Self::Day),
            "MILLI" => Some(Self::Milli),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Sec = 0,
    Millisec = 1,
    Microsec = 2,
    Nanosec = 3,
}
impl TimeUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnit::Sec => "SEC",
            TimeUnit::Millisec => "MILLISEC",
            TimeUnit::Microsec => "MICROSEC",
            TimeUnit::Nanosec => "NANOSEC",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SEC" => Some(Self::Sec),
            "MILLISEC" => Some(Self::Millisec),
            "MICROSEC" => Some(Self::Microsec),
            "NANOSEC" => Some(Self::Nanosec),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IntervalType {
    YearMonth = 0,
    DayTime = 1,
}
impl IntervalType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IntervalType::YearMonth => "YEAR_MONTH",
            IntervalType::DayTime => "DAY_TIME",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "YEAR_MONTH" => Some(Self::YearMonth),
            "DAY_TIME" => Some(Self::DayTime),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SelectionVectorType {
    SvNone = 0,
    SvInt16 = 1,
    SvInt32 = 2,
    SvInt64 = 3,
}
impl SelectionVectorType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SelectionVectorType::SvNone => "SV_NONE",
            SelectionVectorType::SvInt16 => "SV_INT16",
            SelectionVectorType::SvInt32 => "SV_INT32",
            SelectionVectorType::SvInt64 => "SV_INT64",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SV_NONE" => Some(Self::SvNone),
            "SV_INT16" => Some(Self::SvInt16),
            "SV_INT32" => Some(Self::SvInt32),
            "SV_INT64" => Some(Self::SvInt64),
            _ => None,
        }
    }
}
