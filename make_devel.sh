#!/bin/bash

git submodule update --recursive --init

mkdir -p build

cd build

cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_FLAGS="-fmax-errors=1" $@ ..

make -j8

