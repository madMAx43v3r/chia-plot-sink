#!/bin/bash

git submodule update --recursive --init

mkdir -p build

cd build

cmake -DCMAKE_BUILD_TYPE=Release $@ ..

make -j8

