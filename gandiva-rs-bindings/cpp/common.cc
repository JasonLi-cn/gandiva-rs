// Copyright 2024 JasonLi-cn
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

#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "common.h"
#include "config_holder.h"
#include "id_to_module_map.h"
#include "module_holder.h"

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/configuration.h>
#include <gandiva/decimal_scalar.h>
#include <gandiva/filter.h>
#include <gandiva/projector.h>
#include <gandiva/selection_vector.h>
#include <gandiva/tree_expr_builder.h>
#include <gandiva/types.pb.h>

using gandiva::FieldPtr;

namespace gandiva {

// module maps
gandiva::IdToModuleMap<std::shared_ptr<ProjectorHolder>> projector_modules_;
gandiva::IdToModuleMap<std::shared_ptr<FilterHolder>> filter_modules_;

// forward declarations
NodePtr ProtoTypeToNode(const gandiva::types::TreeNode& node);

// Protobuf
bool ParseProtobuf(uint8_t* buf, int buf_len, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream cis(buf, buf_len);
  cis.SetRecursionLimit(2000);
  return msg->ParseFromCodedStream(&cis);
}

DataTypePtr ProtoTypeToTime32(const gandiva::types::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case gandiva::types::SEC:
      return arrow::time32(arrow::TimeUnit::SECOND);
    case gandiva::types::MILLISEC:
      return arrow::time32(arrow::TimeUnit::MILLI);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time32\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToTime64(const gandiva::types::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case gandiva::types::MICROSEC:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case gandiva::types::NANOSEC:
      return arrow::time64(arrow::TimeUnit::NANO);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time64\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToTimestamp(const gandiva::types::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case gandiva::types::SEC:
      return arrow::timestamp(arrow::TimeUnit::SECOND);
    case gandiva::types::MILLISEC:
      return arrow::timestamp(arrow::TimeUnit::MILLI);
    case gandiva::types::MICROSEC:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    case gandiva::types::NANOSEC:
      return arrow::timestamp(arrow::TimeUnit::NANO);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for timestamp\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToInterval(const gandiva::types::ExtGandivaType& ext_type) {
  switch (ext_type.intervaltype()) {
    case gandiva::types::YEAR_MONTH:
      return arrow::month_interval();
    case gandiva::types::DAY_TIME:
      return arrow::day_time_interval();
    default:
      std::cerr << "Unknown interval type: " << ext_type.intervaltype() << "\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToDataType(const gandiva::types::ExtGandivaType& ext_type) {
  switch (ext_type.type()) {
    case gandiva::types::NONE:
      return arrow::null();
    case gandiva::types::BOOL:
      return arrow::boolean();
    case gandiva::types::UINT8:
      return arrow::uint8();
    case gandiva::types::INT8:
      return arrow::int8();
    case gandiva::types::UINT16:
      return arrow::uint16();
    case gandiva::types::INT16:
      return arrow::int16();
    case gandiva::types::UINT32:
      return arrow::uint32();
    case gandiva::types::INT32:
      return arrow::int32();
    case gandiva::types::UINT64:
      return arrow::uint64();
    case gandiva::types::INT64:
      return arrow::int64();
    case gandiva::types::HALF_FLOAT:
      return arrow::float16();
    case gandiva::types::FLOAT:
      return arrow::float32();
    case gandiva::types::DOUBLE:
      return arrow::float64();
    case gandiva::types::UTF8:
      return arrow::utf8();
    case gandiva::types::BINARY:
      return arrow::binary();
    case gandiva::types::DATE32:
      return arrow::date32();
    case gandiva::types::DATE64:
      return arrow::date64();
    case gandiva::types::DECIMAL:
      // TODO: error handling
      return arrow::decimal(ext_type.precision(), ext_type.scale());
    case gandiva::types::TIME32:
      return ProtoTypeToTime32(ext_type);
    case gandiva::types::TIME64:
      return ProtoTypeToTime64(ext_type);
    case gandiva::types::TIMESTAMP:
      return ProtoTypeToTimestamp(ext_type);
    case gandiva::types::INTERVAL:
      return ProtoTypeToInterval(ext_type);
    case gandiva::types::FIXED_SIZE_BINARY:
    case gandiva::types::LIST:
    case gandiva::types::STRUCT:
    case gandiva::types::UNION:
    case gandiva::types::DICTIONARY:
    case gandiva::types::MAP:
      std::cerr << "Unhandled data type: " << ext_type.type() << "\n";
      return nullptr;

    default:
      std::cerr << "Unknown data type: " << ext_type.type() << "\n";
      return nullptr;
  }
}

FieldPtr ProtoTypeToField(const gandiva::types::Field& f) {
  const std::string& name = f.name();
  DataTypePtr type = ProtoTypeToDataType(f.type());
  bool nullable = true;
  if (f.has_nullable()) {
    nullable = f.nullable();
  }

  return field(name, type, nullable);
}

NodePtr ProtoTypeToFieldNode(const gandiva::types::FieldNode& node) {
  FieldPtr field_ptr = ProtoTypeToField(node.field());
  if (field_ptr == nullptr) {
    std::cerr << "Unable to create field node from protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeField(field_ptr);
}

NodePtr ProtoTypeToFnNode(const gandiva::types::FunctionNode& node) {
  const std::string& name = node.functionname();
  NodeVector children;

  for (int i = 0; i < node.inargs_size(); i++) {
    const gandiva::types::TreeNode& arg = node.inargs(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for function: " << name << "\n";
      return nullptr;
    }

    children.push_back(n);
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for function: " << name << "\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeFunction(name, children, return_type);
}

NodePtr ProtoTypeToIfNode(const gandiva::types::IfNode& node) {
  NodePtr cond = ProtoTypeToNode(node.cond());
  if (cond == nullptr) {
    std::cerr << "Unable to create cond node for if node\n";
    return nullptr;
  }

  NodePtr then_node = ProtoTypeToNode(node.thennode());
  if (then_node == nullptr) {
    std::cerr << "Unable to create then node for if node\n";
    return nullptr;
  }

  NodePtr else_node = ProtoTypeToNode(node.elsenode());
  if (else_node == nullptr) {
    std::cerr << "Unable to create else node for if node\n";
    return nullptr;
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for if node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeIf(cond, then_node, else_node, return_type);
}

NodePtr ProtoTypeToAndNode(const gandiva::types::AndNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const gandiva::types::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean and\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeAnd(children);
}

NodePtr ProtoTypeToOrNode(const gandiva::types::OrNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const gandiva::types::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean or\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeOr(children);
}

NodePtr ProtoTypeToInNode(const gandiva::types::InNode& node) {
  NodePtr field = ProtoTypeToNode(node.node());

  if (node.has_int32values()) {
    std::unordered_set<int32_t> int_values;
    for (int i = 0; i < node.int32values().int32values_size(); i++) {
      int_values.insert(node.int32values().int32values(i).value());
    }
    return TreeExprBuilder::MakeInExpressionInt32(field, int_values);
  }

  if (node.has_int64values()) {
    std::unordered_set<int64_t> int64_values;
    for (int i = 0; i < node.int64values().int64values_size(); i++) {
      int64_values.insert(node.int64values().int64values(i).value());
    }
    return TreeExprBuilder::MakeInExpressionInt64(field, int64_values);
  }

  if (node.has_decimalvalues()) {
    std::unordered_set<gandiva::DecimalScalar128> decimal_values;
    for (int i = 0; i < node.decimalvalues().decimalvalues_size(); i++) {
      decimal_values.insert(
          gandiva::DecimalScalar128(node.decimalvalues().decimalvalues(i).value(),
                                    node.decimalvalues().decimalvalues(i).precision(),
                                    node.decimalvalues().decimalvalues(i).scale()));
    }
    return TreeExprBuilder::MakeInExpressionDecimal(field, decimal_values);
  }

  if (node.has_float32values()) {
    std::unordered_set<float> float_values;
    for (int i = 0; i < node.float32values().float32values_size(); i++) {
      float_values.insert(node.float32values().float32values(i).value());
    }
    return TreeExprBuilder::MakeInExpressionFloat(field, float_values);
  }

  if (node.has_float64values()) {
    std::unordered_set<double> double_values;
    for (int i = 0; i < node.float64values().float64values_size(); i++) {
      double_values.insert(node.float64values().float64values(i).value());
    }
    return TreeExprBuilder::MakeInExpressionDouble(field, double_values);
  }

  if (node.has_stringvalues()) {
    std::unordered_set<std::string> stringvalues;
    for (int i = 0; i < node.stringvalues().stringvalues_size(); i++) {
      stringvalues.insert(node.stringvalues().stringvalues(i).value());
    }
    return TreeExprBuilder::MakeInExpressionString(field, stringvalues);
  }

  if (node.has_binaryvalues()) {
    std::unordered_set<std::string> stringvalues;
    for (int i = 0; i < node.binaryvalues().binaryvalues_size(); i++) {
      stringvalues.insert(node.binaryvalues().binaryvalues(i).value());
    }
    return TreeExprBuilder::MakeInExpressionBinary(field, stringvalues);
  }
  // not supported yet.
  std::cerr << "Unknown constant type for in expression.\n";
  return nullptr;
}

NodePtr ProtoTypeToNullNode(const gandiva::types::NullNode& node) {
  DataTypePtr data_type = ProtoTypeToDataType(node.type());
  if (data_type == nullptr) {
    std::cerr << "Unknown type " << data_type->ToString() << " for null node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeNull(data_type);
}

NodePtr ProtoTypeToNode(const gandiva::types::TreeNode& node) {
  if (node.has_fieldnode()) {
    return ProtoTypeToFieldNode(node.fieldnode());
  }

  if (node.has_fnnode()) {
    return ProtoTypeToFnNode(node.fnnode());
  }

  // control expressions
  if (node.has_ifnode()) {
    return ProtoTypeToIfNode(node.ifnode());
  }

  if (node.has_andnode()) {
    return ProtoTypeToAndNode(node.andnode());
  }

  if (node.has_ornode()) {
    return ProtoTypeToOrNode(node.ornode());
  }

  // in expr
  if (node.has_innode()) {
    return ProtoTypeToInNode(node.innode());
  }

  // literals
  if (node.has_nullnode()) {
    return ProtoTypeToNullNode(node.nullnode());
  }

  if (node.has_booleannode()) {
    return TreeExprBuilder::MakeLiteral(node.booleannode().value());
  }

  if (node.has_uint8node()) {
    uint8_t value = node.uint8node().value();
    return TreeExprBuilder::MakeLiteral(value);
  }

  if (node.has_uint16node()) {
    uint16_t value = node.uint16node().value();
    return TreeExprBuilder::MakeLiteral(value);
  }

  if (node.has_uint32node()) {
    return TreeExprBuilder::MakeLiteral(node.uint32node().value());
  }

  if (node.has_uint64node()) {
    return TreeExprBuilder::MakeLiteral(node.uint64node().value());
  }

  if (node.has_int8node()) {
    int8_t value = node.int8node().value();
    return TreeExprBuilder::MakeLiteral(value);
  }

  if (node.has_int16node()) {
    int16_t value = node.int16node().value();
    return TreeExprBuilder::MakeLiteral(value);
  }

  if (node.has_int32node()) {
    return TreeExprBuilder::MakeLiteral(node.int32node().value());
  }

  if (node.has_int64node()) {
    return TreeExprBuilder::MakeLiteral(node.int64node().value());
  }

  if (node.has_float32node()) {
    return TreeExprBuilder::MakeLiteral(node.float32node().value());
  }

  if (node.has_float64node()) {
    return TreeExprBuilder::MakeLiteral(node.float64node().value());
  }

  if (node.has_stringnode()) {
    return TreeExprBuilder::MakeStringLiteral(node.stringnode().value());
  }

  if (node.has_binarynode()) {
    return TreeExprBuilder::MakeBinaryLiteral(node.binarynode().value());
  }

  if (node.has_decimalnode()) {
    std::string value = node.decimalnode().value();
    gandiva::DecimalScalar128 literal(value, node.decimalnode().precision(),
                                      node.decimalnode().scale());
    return TreeExprBuilder::MakeDecimalLiteral(literal);
  }
  std::cerr << "Unknown node type in protobuf\n";
  return nullptr;
}

ExpressionPtr ProtoTypeToExpression(const gandiva::types::ExpressionRoot& root) {
  NodePtr root_node = ProtoTypeToNode(root.root());
  if (root_node == nullptr) {
    std::cerr << "Unable to create expression node from expression protobuf\n";
    return nullptr;
  }

  FieldPtr field = ProtoTypeToField(root.resulttype());
  if (field == nullptr) {
    std::cerr << "Unable to extra return field from expression protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeExpression(root_node, field);
}

ConditionPtr ProtoTypeToCondition(const gandiva::types::Condition& condition) {
  NodePtr root_node = ProtoTypeToNode(condition.root());
  if (root_node == nullptr) {
    return nullptr;
  }

  return TreeExprBuilder::MakeCondition(root_node);
}

SchemaPtr ProtoTypeToSchema(const gandiva::types::Schema& schema) {
  std::vector<FieldPtr> fields;

  for (int i = 0; i < schema.columns_size(); i++) {
    FieldPtr field = ProtoTypeToField(schema.columns(i));
    if (field == nullptr) {
      std::cerr << "Unable to extract arrow field from schema\n";
      return nullptr;
    }

    fields.push_back(field);
  }

  return arrow::schema(fields);
}

class RustResizableBuffer : public arrow::ResizableBuffer {
 public:
  RustResizableBuffer(RustMutableBuffer* mutable_buffer,
                      RustMutableBuffers* mutable_buffers,
                      rust_mutable_buffer_reserve_fn reserve_fn, uintptr_t idx)
      : arrow::ResizableBuffer(reinterpret_cast<uint8_t*>(mutable_buffer->addr),
                               mutable_buffer->size),
        mutable_buffer_(mutable_buffer),
        mutable_buffers_(mutable_buffers),
        reserve_fn_(reserve_fn),
        idx_(idx) {}

  Status Resize(const int64_t new_size, bool shrink_to_fit) override;

  Status Reserve(const int64_t new_capacity) override;

 private:
  uintptr_t idx_;
  RustMutableBuffer* mutable_buffer_;
  RustMutableBuffers* mutable_buffers_;
  rust_mutable_buffer_reserve_fn reserve_fn_;
};

Status RustResizableBuffer::Resize(const int64_t new_size, bool shrink_to_fit) {
  if (shrink_to_fit) {
    return Status::NotImplemented("shrink not implemented");
  }

  if (ARROW_PREDICT_TRUE(new_size <= capacity())) {
    // no need to expand.
    size_ = new_size;
    return Status::OK();
  }

  RETURN_NOT_OK(Reserve(new_size));
  DCHECK_GE(capacity_, new_size);
  size_ = new_size;
  return Status::OK();
}

Status RustResizableBuffer::Reserve(const int64_t new_capacity) {
  reserve_fn_(mutable_buffers_, idx_, mutable_buffer_, new_capacity);
  data_ = reinterpret_cast<uint8_t*>(mutable_buffer_->addr);
  capacity_ = mutable_buffer_->capacity;
  return Status::OK();
}

Status make_record_batch_with_buf_addrs(const SchemaPtr& schema, int64_t num_rows,
                                        const RustBuffer* in_bufs, int32_t in_bufs_len,
                                        std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;

    // 1 validity buffer
    if (buf_idx >= in_bufs_len) {
      return Status::Invalid("insufficient number of in_buf_addrs");
    }
    auto validity_buf = in_bufs[buf_idx++];

    uintptr_t validity_addr = validity_buf.addr;
    int64_t validity_size = validity_buf.size;
    if (validity_size == 0) {
      buffers.push_back(nullptr);
    } else {
      auto validity = std::make_shared<arrow::Buffer>(
          reinterpret_cast<uint8_t*>(validity_addr), validity_size);
      buffers.push_back(validity);
    }

    // 2 offsets buffer
    if (arrow::is_binary_like(field->type()->id())) {
      if (buf_idx >= in_bufs_len) {
        return Status::Invalid("insufficient number of in_buf_addrs");
      }

      // add offsets buffer for variable-len fields.
      auto offsets_buf = in_bufs[buf_idx++];
      uintptr_t offsets_addr = offsets_buf.addr;
      int64_t offsets_size = offsets_buf.size;
      auto offsets = std::make_shared<arrow::Buffer>(
          reinterpret_cast<uint8_t*>(offsets_addr), offsets_size);
      buffers.push_back(offsets);
    }

    // 3 value buffer
    if (buf_idx >= in_bufs_len) {
      return Status::Invalid("insufficient number of in_buf_addrs");
    }
    auto value_buf = in_bufs[buf_idx++];
    uintptr_t value_addr = value_buf.addr;
    int64_t value_size = value_buf.size;
    auto data = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(value_addr),
                                                value_size);
    buffers.push_back(data);

    auto array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
    columns.push_back(array_data);
  }

  *batch = arrow::RecordBatch::Make(schema, num_rows, columns);
  return Status::OK();
}

ModuleId BuildProjector(ProtoMessage pb_schema, ProtoMessage pb_exprs,
                        SelectionVectorType selection_vector_type, ConfigId config_id) {
  // schema
  gandiva::types::Schema schema;
  if (!ParseProtobuf(pb_schema.addr, pb_schema.len, &schema)) {
    std::cout << "Unable to parse schema protobuf\n";
  }
  SchemaPtr schema_ptr;
  schema_ptr = ProtoTypeToSchema(schema);
  if (schema_ptr == nullptr) {
    std::cout << "Unable to construct arrow schema object from schema protobuf\n";
  }

  // exprs
  gandiva::types::ExpressionList exprs;
  if (!ParseProtobuf(pb_exprs.addr, pb_exprs.len, &exprs)) {
    std::cout << "Unable to parse expressions protobuf\n";
  }
  ExpressionVector expr_vector;
  FieldVector ret_types;
  for (int i = 0; i < exprs.exprs_size(); i++) {
    ExpressionPtr root = ProtoTypeToExpression(exprs.exprs(i));
    if (root == nullptr) {
      std::cout << "Unable to construct expression object from expression protobuf\n";
    }
    expr_vector.push_back(root);
    ret_types.push_back(root->result());
  }

  // config
  std::shared_ptr<Configuration> config = ConfigHolder::MapLookup(config_id);

  // selection
  auto mode = gandiva::SelectionVector::MODE_NONE;
  switch (selection_vector_type) {
    case gandiva::types::SV_NONE:
      mode = gandiva::SelectionVector::MODE_NONE;
      break;
    case gandiva::types::SV_INT16:
      mode = gandiva::SelectionVector::MODE_UINT16;
      break;
    case gandiva::types::SV_INT32:
      mode = gandiva::SelectionVector::MODE_UINT32;
      break;
    case gandiva::types::SV_INT64:
      mode = gandiva::SelectionVector::MODE_UINT64;
      break;
  }

  // good to invoke the evaluator now
  std::shared_ptr<Projector> projector;
  gandiva::Status status =
      Projector::Make(schema_ptr, expr_vector, mode, config, &projector);
  if (!status.ok()) {
    std::cout << "Failed to make LLVM module due to " << status.message() << "\n";
  }

  // store the result in a map
  std::shared_ptr<ProjectorHolder> holder =
      std::make_shared<ProjectorHolder>(schema_ptr, ret_types, std::move(projector));
  ModuleId module_id = projector_modules_.Insert(holder);

  // TODO release
  // releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);

  return module_id;
}

#define CHECK_OUT_BUFFER_IDX_AND_BREAK(idx, len)                               \
  if (idx >= len) {                                                            \
    status = gandiva::Status::Invalid("insufficient number of out_buf_addrs"); \
    break;                                                                     \
  }

void EvaluateProjector(ModuleId module_id, int64_t num_rows, RustBuffer* in_bufs,
                       int32_t in_bufs_len, SelectionVectorInfo sel_vec_info,
                       RustMutableBuffer* out_bufs, int32_t out_bufs_len,
                       RustMutableBuffers* mutable_buffers,
                       rust_mutable_buffer_reserve_fn reserve_fn) {
  // get holder
  std::shared_ptr<ProjectorHolder> holder = projector_modules_.Lookup(module_id);
  if (holder == nullptr) {
    std::stringstream ss;
    ss << "Unknown module id " << module_id;
    return;
  }

  Status status;
  do {
    // prepare in batch
    std::shared_ptr<arrow::RecordBatch> in_batch;
    status = make_record_batch_with_buf_addrs(holder->schema(), num_rows, in_bufs,
                                              in_bufs_len, &in_batch);
    if (!status.ok()) {
      break;
    }

    // selection
    std::shared_ptr<gandiva::SelectionVector> selection_vector;
    auto selection_buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<uint8_t*>(sel_vec_info.addr), sel_vec_info.size);
    int64_t output_row_count = 0;
    switch (sel_vec_info.type) {
      case gandiva::types::SV_NONE: {
        output_row_count = num_rows;
        break;
      }
      case gandiva::types::SV_INT16: {
        status = gandiva::SelectionVector::MakeImmutableInt16(
            sel_vec_info.rows, selection_buffer, &selection_vector);
        output_row_count = sel_vec_info.rows;
        break;
      }
      case gandiva::types::SV_INT32: {
        status = gandiva::SelectionVector::MakeImmutableInt32(
            sel_vec_info.rows, selection_buffer, &selection_vector);
        output_row_count = sel_vec_info.rows;
        break;
      }
      case gandiva::types::SV_INT64: {
        status = gandiva::SelectionVector::MakeInt64(sel_vec_info.rows, selection_buffer,
                                                     &selection_vector);
        output_row_count = sel_vec_info.rows;
        break;
      }
    }
    if (!status.ok()) {
      break;
    }

    // result types
    auto ret_types = holder->rettypes();
    ArrayDataVector output;
    int buf_idx = 0;
    int output_vector_idx = 0;

    // out array
    for (const FieldPtr& field : ret_types) {
      std::vector<std::shared_ptr<arrow::Buffer>> buffers;

      CHECK_OUT_BUFFER_IDX_AND_BREAK(buf_idx, out_bufs_len);
      auto validity_buf = out_bufs[buf_idx++];
      uintptr_t validity_addr = validity_buf.addr;
      int64_t validity_size = validity_buf.size;
      buffers.push_back(std::make_shared<arrow::MutableBuffer>(
          reinterpret_cast<uint8_t*>(validity_addr), validity_size));

      if (arrow::is_binary_like(field->type()->id())) {
        CHECK_OUT_BUFFER_IDX_AND_BREAK(buf_idx, out_bufs_len);
        auto offsets_buf = out_bufs[buf_idx++];
        uintptr_t offsets_addr = offsets_buf.addr;
        int64_t offsets_size = offsets_buf.size;
        buffers.push_back(std::make_shared<arrow::MutableBuffer>(
            reinterpret_cast<uint8_t*>(offsets_addr), offsets_size));
      }

      CHECK_OUT_BUFFER_IDX_AND_BREAK(buf_idx, out_bufs_len);
      uintptr_t idx = buf_idx;
      auto value_buf = out_bufs[buf_idx++];
      if (arrow::is_binary_like(field->type()->id())) {
        if (reserve_fn == nullptr) {
          status = Status::Invalid(
              "expression has variable len output columns, but the expander object is "
              "null");
          break;
        }
        buffers.push_back(std::make_shared<RustResizableBuffer>(
            &value_buf, mutable_buffers, reserve_fn, idx));
      } else {
        uintptr_t data_addr = value_buf.addr;
        int64_t data_size = value_buf.size;
        buffers.push_back(std::make_shared<arrow::MutableBuffer>(
            reinterpret_cast<uint8_t*>(data_addr), data_size));
      }

      auto array_data = arrow::ArrayData::Make(field->type(), output_row_count, buffers);
      output.push_back(array_data);
      ++output_vector_idx;
    }

    if (!status.ok()) {
      break;
    }

    status = holder->projector()->Evaluate(*in_batch, selection_vector.get(), output);
  } while (false);

  if (!status.ok()) {
    std::stringstream ss;
    ss << "Evaluate returned " << status.message() << "\n";
    return;
  }
}

void CloseProjector(ModuleId module_id) { projector_modules_.Erase(module_id); }

/// Filter

ModuleId BuildFilter(ProtoMessage pb_schema, ProtoMessage pb_condition,
                     ConfigId config_id) {
  // schema
  gandiva::types::Schema schema;
  if (!ParseProtobuf(pb_schema.addr, pb_schema.len, &schema)) {
    std::cout << "Unable to parse schema protobuf\n";
  }
  SchemaPtr schema_ptr;
  schema_ptr = ProtoTypeToSchema(schema);
  if (schema_ptr == nullptr) {
    std::cout << "Unable to construct arrow schema object from schema protobuf\n";
  }

  // condition
  gandiva::types::Condition condition;
  if (!ParseProtobuf(pb_condition.addr, pb_condition.len, &condition)) {
    std::cout << "Unable to parse condition protobuf\n";
  }
  ConditionPtr condition_ptr;
  condition_ptr = ProtoTypeToCondition(condition);
  if (condition_ptr == nullptr) {
    std::cout << "Unable to construct condition object from condition protobuf\n";
  }

  // config
  std::shared_ptr<Configuration> config = ConfigHolder::MapLookup(config_id);

  std::shared_ptr<Filter> filter;
  gandiva::Status status;
  // good to invoke the filter builder now
  status = Filter::Make(schema_ptr, condition_ptr, config, &filter);
  if (!status.ok()) {
    std::cout << "Failed to make LLVM module due to " << status.message() << "\n";
  }

  // store the result in a map
  ModuleId module_id = 0LL;
  std::shared_ptr<FilterHolder> holder;
  holder = std::make_shared<FilterHolder>(schema_ptr, std::move(filter));
  module_id = filter_modules_.Insert(holder);

  // TODO releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes,
  // env);
  return module_id;
}

int32_t EvaluateFilter(ModuleId module_id, int64_t num_rows, RustBuffer* in_bufs,
                       int32_t in_bufs_len, SelectionVectorType selection_vector_type,
                       RustBuffer out_buf) {
  gandiva::Status status;
  std::shared_ptr<FilterHolder> holder = filter_modules_.Lookup(module_id);
  if (holder == nullptr) {
    return -1;
  }

  std::shared_ptr<gandiva::SelectionVector> selection_vector;
  do {
    std::shared_ptr<arrow::RecordBatch> in_batch;
    status = make_record_batch_with_buf_addrs(holder->schema(), num_rows, in_bufs,
                                              in_bufs_len, &in_batch);
    if (!status.ok()) {
      break;
    }

    auto out_buffer = std::make_shared<arrow::MutableBuffer>(
        reinterpret_cast<uint8_t*>(out_buf.addr), out_buf.size);
    switch (selection_vector_type) {
      case gandiva::types::SV_INT16:
        status =
            gandiva::SelectionVector::MakeInt16(num_rows, out_buffer, &selection_vector);
        break;
      case gandiva::types::SV_INT32:
        status =
            gandiva::SelectionVector::MakeInt32(num_rows, out_buffer, &selection_vector);
        break;
      case gandiva::types::SV_INT64:
        status =
            gandiva::SelectionVector::MakeInt64(num_rows, out_buffer, &selection_vector);
        break;
      default:
        status = gandiva::Status::Invalid("unknown selection vector type");
    }
    if (!status.ok()) {
      break;
    }

    status = holder->filter()->Evaluate(*in_batch, selection_vector);
  } while (false);

  // TODO
  // env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  // env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    std::cout << "Evaluate returned " << status.message() << "\n";
    return -1;
  } else {
    int64_t num_slots = selection_vector->GetNumSlots();
    // Check integer overflow
    if (num_slots > INT_MAX) {
      std::cout << "The selection vector has " << num_slots
                << " slots, which is larger than the " << INT_MAX << " limit.\n";
      return -1;
    }
    return static_cast<int>(num_slots);
  }
}

void CloseFilter(ModuleId module_id) { filter_modules_.Erase(module_id); }

}  // namespace gandiva
