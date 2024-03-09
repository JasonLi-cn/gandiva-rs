# Copyright 2024 JasonLi-cn
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ref: https://arrow.apache.org/docs/cpp/build_system.html#cmake

cmake_minimum_required(VERSION 3.16)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# find_package() uses <PackageName>_ROOT variables.
# https://cmake.org/cmake/help/latest/policy/CMP0074.html
if(POLICY CMP0074)
    cmake_policy(SET CMP0074 NEW)
endif()

project(arrow_gandiva_rs)

include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

if(NOT ARROW_CPP_BUILD_DIR)
    message(FATAL_ERROR "ARROW_CPP_BUILD_DIR is not satisfied. Configuration will terminate.")
endif()

if(NOT ARROW_CPP_INSTALL_LIBDIR)
    message(FATAL_ERROR "ARROW_CPP_INSTALL_LIBDIR is not satisfied. Configuration will terminate.")
endif()

set(ARROW_ROOT "${ARROW_CPP_INSTALL_LIBDIR}/cmake")
set(CMAKE_PREFIX_PATH ${ARROW_ROOT} ${CMAKE_PREFIX_PATH})

# dependence

## gandiva
find_package(Gandiva REQUIRED)

## protobuf
set(GANDIVA_PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(GANDIVA_PROTO_OUTPUT_FILES "${GANDIVA_PROTO_OUTPUT_DIR}/gandiva/types.pb.cc"
        "${GANDIVA_PROTO_OUTPUT_DIR}/gandiva/types.pb.h")

set_source_files_properties(${GANDIVA_PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(GANDIVA_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../proto ABSOLUTE)
get_filename_component(GANDIVA_PROTO_FILE_ABSOLUTE ${GANDIVA_PROTO_DIR}/gandiva/types.proto ABSOLUTE)
get_filename_component(PROTOBUF_ROOT_ABSOLUTE ${ARROW_CPP_BUILD_DIR}/protobuf_ep-install ABSOLUTE)

set(Protobuf_USE_STATIC_LIBS ON)
set(Protobuf_ROOT ${PROTOBUF_ROOT_ABSOLUTE})
set(Protobuf_PROTOC_EXECUTABLE ${Protobuf_ROOT}/bin/protoc)

find_package(Protobuf REQUIRED PATHS ${Protobuf_ROOT}/lib/cmake)
include_directories(${Protobuf_ROOT}/include)

add_custom_command(OUTPUT ${GANDIVA_PROTO_OUTPUT_FILES}
        COMMAND protobuf::protoc --proto_path ${GANDIVA_PROTO_DIR} --cpp_out
        ${GANDIVA_PROTO_OUTPUT_DIR} ${GANDIVA_PROTO_FILE_ABSOLUTE}
        DEPENDS ${GANDIVA_PROTO_FILE_ABSOLUTE}
        COMMENT "Running Protobuf compiler on gandiva/types.proto"
        VERBATIM)

# libs
add_library(arrow_gandiva_rs STATIC
        wrapper.h
        base.h
        common.h
        common.cc
        config_builder.h
        config_builder.cc
        config_holder.h
        config_holder.cc
        id_to_module_map.h
        module_holder.h
        ${GANDIVA_PROTO_OUTPUT_FILES}
        )

target_link_libraries(arrow_gandiva_rs
        protobuf::libprotobuf
        Gandiva::gandiva_static
        )

# Tests
