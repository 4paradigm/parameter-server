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

find_package(Jemalloc REQUIRED)
find_package(PicoCoreDep REQUIRED)

file(GLOB PS_SRC api/*.cpp common/*.cpp handler/*.cpp model/*.cpp native_ps/*.cpp operator/*.cpp service/*.cpp storage/*.cpp service/coordinated_restore/*.cpp controller/*.cpp)

add_library(pico_ps_obj OBJECT ${PS_SRC})

add_library(pico_ps_static STATIC $<TARGET_OBJECTS:pico_ps_obj>)
target_link_libraries(pico_ps_static PUBLIC pico_core_static ${PicoCoreDep_STATIC_LIBRARIES} ${RDMA_LIBRARIES} ${Jemalloc_pic_LIBRARIES} ${PMEM_STATIC_LIBRARIES} ${NDCTL_LIBRARIES} dl)

add_library(pico_ps SHARED $<TARGET_OBJECTS:pico_ps_obj>)
target_link_libraries(pico_ps PUBLIC pico_core ${PicoCoreDep_LIBRARIES} ${RDMA_LIBRARIES} ${Jemalloc_pic_LIBRARIES} ${PMEM_LIBRARIES} ${NDCTL_LIBRARIES} dl)

if (NOT SKIP_BUILD_TEST)
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

