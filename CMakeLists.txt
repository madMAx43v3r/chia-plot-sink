cmake_minimum_required(VERSION 3.10)
project(chia-plot-sink CXX C)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)

include_directories(include)

add_executable(chia_plot_sink src/chia_plot_sink.cpp)

target_link_libraries(chia_plot_sink stdc++fs Threads::Threads)
