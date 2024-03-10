#!/bin/bash

BIN_DIR=$(cd `dirname $0`; pwd)
PROJ_DIR=$(cd `dirname $BIN_DIR`; pwd)

cd $PROJ_DIR/cpp

cmake -B $PROJ_DIR/cpp-build \
-DARROW_CPP_BUILD_DIR=$PROJ_DIR/arrow-cpp-build \
-DARROW_CPP_INSTALL_LIBDIR=$PROJ_DIR/arrow-cpp-install/lib

cmake \
--build $PROJ_DIR/cpp-build \
--config Release
