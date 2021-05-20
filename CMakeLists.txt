cmake_minimum_required(VERSION 3.10)

project(pico-ps VERSION 0.1.0)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake ${PROJECT_SOURCE_DIR}/pico-core/cmake)

if (THIRD_PARTY)
    set(CMAKE_PREFIX_PATH "${THIRD_PARTY}")
    message(STATUS "THIRD_PARTY=${THIRD_PARTY}")
    include_directories(SYSTEM ${THIRD_PARTY}/include)
    link_directories(${THIRD_PARTY}/lib ${THIRD_PARTY}/lib64)
endif()

option(SKIP_BUILD_TEST "whether skip build testcase" OFF)
message(STATUS "SKIP_BUILD_TEST: ${SKIP_BUILD_TEST}")

# check gcc version
if(CMAKE_COMPILER_IS_GNUCXX)
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
    message(STATUS "gcc ${GCC_VERSION}")
    if(GCC_VERSION VERSION_GREATER 5.2.0 OR GCC_VERSION VERSION_EQUAL 5.2.0)
        message(STATUS "C++14 activated.")
    else()
        message(FATAL_ERROR "C++14 needed. Therefore a gcc compiler with a version higher than 5.2 is needed.")
    endif()
else()
    message(FATAL_ERROR "only gcc supported")
endif()

add_definitions(--std=c++14 -Wall -Wextra -Wno-deprecated-declarations -Werror -frecord-gcc-switches -fPIC)
include_directories(SYSTEM ${PROJECT_SOURCE_DIR})

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

