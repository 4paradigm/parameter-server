find_lib(LIBYAMLCPP STATIC LIBS yaml-cpp)

add_library(TestOpObjects OBJECT TestOps.cpp TestPredictorOps.cpp)
add_library(TestOps INTERFACE)
target_link_libraries(TestOps INTERFACE $<TARGET_OBJECTS:TestOpObjects> TestOpObjects pico_ps_static ${LIBYAMLCPP})

link_libraries(pico_ps_static gtest gtest_main)

# 服务化功能测试
# ps_ha_loader_puller_test

include(GoogleTest)

add_executable(ps_service_test ps_service_test.cpp)
add_executable(ps_predictor_native_ps_test ps_predictor_native_ps_test.cpp)
add_executable(ps_ha_loader_puller_test ps_ha_loader_puller_test.cpp)
gtest_discover_tests(ps_service_test)
gtest_discover_tests(ps_predictor_native_ps_test)
gtest_discover_tests(ps_ha_loader_puller_test)

if (USE_DCPMM)
    add_executable(ps_pmem_test ps_pmem_test.cpp)
    gtest_discover_tests(ps_pmem_test)
endif()

# add_subdirectory(old)
