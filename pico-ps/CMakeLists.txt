cmake_minimum_required(VERSION 3.10)
project(pico-ps)

option(SKIP_BUILD_TEST "whether skip build testcase" OFF)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake ${PROJECT_SOURCE_DIR}/../pico-core/cmake)
set(CMAKE_PREFIX_PATH "${THIRD_PARTY}")

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

option(USE_RDMA "whether build with rdma support" OFF)
if (USE_RDMA)
    add_definitions(-DUSE_RDMA)
    set(RDMA_LIBRARIES rdmacm ibverbs)
    message(STATUS "RDMA enabled")
else()
    message(STATUS "RDMA disabled")
    set(RDMA_LIBRARIES )
endif()

option(USE_DCPMM "whether build with dcpmm support" OFF)
if (USE_DCPMM)
    add_definitions(-DUSE_DCPMM)
    find_package(PMEM REQUIRED)
    message(STATUS "DCPMM enabled")
else()
    message(STATUS "DCPMM disabled")
endif()

if (DEBUG)
    add_definitions(-O0 -g)
else()
    add_definitions(-O3 -DNDEBUG)
endif()

set(CMAKE_SHARED_LINKER_FLAGS "-pthread -Wl,--whole-archive -lrt -Wl,--no-whole-archive")
set(CMAKE_EXE_LINKER_FLAGS "-pthread -Wl,--whole-archive -lrt -Wl,--no-whole-archive")

find_package(Jemalloc REQUIRED)
find_package(PicoCoreDep REQUIRED)

file(GLOB PS_SRC api/*.cpp common/*.cpp handler/*.cpp model/*.cpp native_ps/*.cpp operator/*.cpp service/*.cpp storage/*.cpp service/coordinated_restore/*.cpp controller/*.cpp)
add_library(pico_ps_obj OBJECT ${PS_SRC})

add_library(pico_ps_static STATIC $<TARGET_OBJECTS:pico_ps_obj>)
target_link_libraries(pico_ps_static PUBLIC pico_core_static ${PicoCoreDep_STATIC_LIBRARIES} ${RDMA_LIBRARIES} ${Jemalloc_pic_LIBRARIES} ${PMEM_STATIC_LIBRARIES} ${NDCTL_LIBRARIES} dl)

add_library(pico_ps SHARED $<TARGET_OBJECTS:pico_ps_obj>)
target_link_libraries(pico_ps PUBLIC pico_core ${PicoCoreDep_LIBRARIES} ${RDMA_LIBRARIES} ${Jemalloc_pic_LIBRARIES} ${PMEM_LIBRARIES} ${NDCTL_LIBRARIES} dl)

if (NOT SKIP_BUILD_TEST)
    enable_testing()
    add_subdirectory(test)
endif()

INSTALL(TARGETS pico_ps pico_ps_static
       LIBRARY DESTINATION lib
       ARCHIVE DESTINATION lib
)

INSTALL(DIRECTORY ./ DESTINATION include/pico-ps
        FILES_MATCHING PATTERN "*.h")

INSTALL(DIRECTORY ./ DESTINATION include/pico-ps
        FILES_MATCHING PATTERN "*.hpp")

