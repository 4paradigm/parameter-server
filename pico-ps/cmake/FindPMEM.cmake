include(common)

set(LIBPMEM_REQUIRED_VERSION 1.8)
set(LIBPMEMOBJ_REQUIRED_VERSION 1.8)
set(LIBPMEMOBJ++_REQUIRED_VERSION 1.10)

find_package(PkgConfig QUIET)
if (PKG_CONFIG_FOUND)
    pkg_check_modules(LIBPMEM REQUIRED libpmem>=${LIBPMEM_REQUIRED_VERSION})
    pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj>=${LIBPMEMOBJ_REQUIRED_VERSION})
    pkg_check_modules(LIBPMEMOBJ++ REQUIRED libpmemobj++>=${LIBPMEMOBJ++_REQUIRED_VERSION})
else()
    find_package(LIBPMEM REQUIRED)
    find_package(LIBPMEMOBJ REQUIRED)
    find_package(LIBPMEMOBJ++ REQUIRED)
endif()

find_lib(NDCTL_LIBRARIES SHARED LIBS daxctl ndctl PATHS ${LIB_INSTALL_DIR})
find_lib(PMEM_LIBRARIES SHARED LIBS pmemobj pmem PATHS ${LIB_INSTALL_DIR})
find_lib(PMEM_STATIC_LIBRARIES STATIC LIBS pmemobj pmem PATHS ${LIB_INSTALL_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PMEM DEFAULT_MSG PMEM_LIBRARIES)
find_package_handle_standard_args(PMEM_STATIC DEFAULT_MSG PMEM_STATIC_LIBRARIES)

mark_as_advanced(PMEM_LIBRARIES NDCTL_LIBRARIES)
mark_as_advanced(PMEM_STATIC_LIBRARIES NDCTL_LIBRARIES)
