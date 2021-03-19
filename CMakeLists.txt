cmake_minimum_required(VERSION 3.10)
project(pico-ps)

if(DEFINED THIRD_PARTY)
    set(CMAKE_PREFIX_PATH "${THIRD_PARTY}")
    message(STATUS "THIRD_PARTY=${THIRD_PARTY}")
else()
    set(THIRD_PARTY "/usr")
endif()

option(SKIP_BUILD_TEST "whether skip build testcase" OFF)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake ${PROJECT_SOURCE_DIR}/pico-core/cmake)

message(STATUS "SKIP_BUILD_TEST: ${SKIP_BUILD_TEST}")

if (EXISTS $ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
else()
    execute_process(COMMAND which g++ OUTPUT_VARIABLE CMAKE_CXX_COMPILER)
    string(STRIP ${CMAKE_CXX_COMPILER} CMAKE_CXX_COMPILER)
endif()
if (EXISTS $ENV{CC})
    set(CMAKE_C_COMPILER $ENV{CC})
else()
    execute_process(COMMAND which gcc OUTPUT_VARIABLE CMAKE_C_COMPILER)
    string(STRIP ${CMAKE_C_COMPILER} CMAKE_C_COMPILER)
endif()

add_definitions(--std=c++14 -Wall -Wextra -Wno-deprecated-declarations -Werror -frecord-gcc-switches -fPIC)
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/../ ${THIRD_PARTY}/include)
link_directories(${THIRD_PARTY}/lib ${THIRD_PARTY}/lib64)

if (DEBUG)
    add_definitions(-O0 -g)
else()
    add_definitions(-O3 -DNDEBUG)
endif()

set(CMAKE_SHARED_LINKER_FLAGS "-pthread -Wl,--whole-archive -lrt -Wl,--no-whole-archive")
set(CMAKE_EXE_LINKER_FLAGS "-pthread -Wl,--whole-archive -lrt -Wl,--no-whole-archive")

if (NOT SKIP_BUILD_TEST)
    enable_testing()
endif()
add_subdirectory(pico-ps)

