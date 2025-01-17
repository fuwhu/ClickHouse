#!/bin/bash

echo "start building"

git submodule update --init --recursive
mkdir -p build
cd build
export CC=clang-14
export CXX=clang++-14
cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_LIBHDFS=1
ninja

echo "end build"
