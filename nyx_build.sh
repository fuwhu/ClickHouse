#!/bin/bash

echo "start building"

git submodule sync --recursive
git submodule update --init --recursive
mkdir -p build
cd build
/data/app/cmake/cmake-3.28.3/bin/cmake .. -DCMAKE_C_COMPILER=/usr/bin/clang-19 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-19 -DCMAKE_BUILD_TYPE=Release -DUSE_LIBHDFS=1
ninja

echo "end build"
