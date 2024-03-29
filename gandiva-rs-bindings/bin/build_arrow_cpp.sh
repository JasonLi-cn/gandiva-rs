#!/bin/bash

BIN_DIR=$(cd `dirname $0`; pwd)
PROJ_DIR=$(cd `dirname $BIN_DIR`; pwd)

cd $PROJ_DIR/arrow

cmake \
-S $PROJ_DIR/arrow/cpp \
-B $PROJ_DIR/arrow-cpp-build \
-DARROW_BUILD_SHARED=OFF \
-DARROW_DEPENDENCY_SOURCE=BUNDLED \
-DARROW_DEPENDENCY_USE_SHARED=OFF \
-DARROW_PROTOBUF_USE_SHARED=OFF \
-DARROW_FILESYSTEM=ON \
-DARROW_GANDIVA=ON \
-DARROW_GANDIVA_STATIC_LIBSTDCPP=ON \
-DARROW_WITH_PROTOBUF=ON \
-DARROW_WITH_ZLIB=ON \
-DARROW_WITH_ZSTD=ON \
-DARROW_USE_CCACHE=ON \
-DCMAKE_UNITY_BUILD=OFF \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_LIBDIR=lib \
-DCMAKE_INSTALL_PREFIX=$PROJ_DIR/arrow-cpp-install 

if [ $? -ne 0 ]; then
  exit $?
fi

cmake \
--build $PROJ_DIR/arrow-cpp-build \
--target install \
--config Release
