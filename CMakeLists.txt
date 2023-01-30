cmake_minimum_required(VERSION 3.10)
project(chia-plot-sink CXX C)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)

if(MSVC)
	add_definitions(/openmp)
else()
	find_package(OpenMP REQUIRED)
endif()

include_directories(include)

add_executable(chia_plot_sink src/chia_plot_sink.cpp)
add_executable(chia_plot_copy src/chia_plot_copy.cpp)

if(MSVC)
	target_link_libraries(chia_plot_sink ws2_32)
	target_link_libraries(chia_plot_copy ws2_32)
else()
	target_link_libraries(chia_plot_sink stdc++fs Threads::Threads)
	target_link_libraries(chia_plot_copy stdc++fs OpenMP::OpenMP_CXX)
endif()
