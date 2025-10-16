#!/bin/bash

set -euo pipefail

echo "Starting ClickHouse build process..."

BUILD_DIR="build"
cd "$BUILD_DIR"

# Set compiler environment variables
export CC=clang-19
export CXX=clang++-19

echo "Configuring project with CMake (Release build)..."
cmake .. -DCMAKE_BUILD_TYPE=Release

echo "Building project using Ninja..."
ninja

echo "Build completed successfully!"