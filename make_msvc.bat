@ECHO OFF

mkdir build

cmake -B build -G Ninja -DCMAKE_BUILD_TYPE="Release"
cmake --build build -- -k0
