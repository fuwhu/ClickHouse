#!/bin/bash

echo "start building"

git submodule update --init
mkdir -p build
cd build
export CC=clang-14
export CXX=clang++-14
cmake .. -DCMAKE_BUILD_TYPE=Release
ninja

echo "end build"
