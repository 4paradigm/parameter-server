#!/bin/bash
set -e
PROJECT_ROOT=`pwd`
echo ${PROJECT_ROOT}
#THIRD_PARTY_PREFIX=
#THIRD_PARTY_SRC=
#PREFIX=?
#USE_RDMA=?
#J=?
#PATH=?
#USE_UCPMM=?
#SKIP_BUILD_TEST=?
export PICO_PS_VERSION=0.0.0.0
export PICO_PS_VERSION_CODE=$PICO_PS_VERSION-unknown

function setup() {
    if [ 0"${THIRD_PARTY_SRC}" == "0" ]; then
        #git submodule update --init --recursive --checkout
        THIRD_PARTY_SRC=${PROJECT_ROOT}/third-party
    fi
    if [ 0"${THIRD_PARTY_PREFIX}" == "0" ]; then
        THIRD_PARTY_PREFIX=${PROJECT_ROOT}/third-party
    fi
    if [ 0"${PICO_CORE_SRC}" == "0" ]; then
        PICO_CORE_SRC=${PROJECT_ROOT}/pico-core
        pushd $PICO_CORE_SRC
        prefix=${THIRD_PARTY_PREFIX} THIRD_PARTY_SRC=$THIRD_PARTY_SRC THIRD_PARTY_PREFIX=$THIRD_PARTY_PREFIX USE_RDMA=$USE_RDMA SKIP_BUILD_TEST=1 ./build.sh
        popd
    fi
    # install tools 
    if [ "${USE_DCPMM}" == "1" ]; then
        prefix=${THIRD_PARTY_PREFIX} ${THIRD_PARTY_SRC}/prepare.sh build  cmake glog gflags yaml boost zookeeper zlib snappy lz4 jemalloc sparsehash googletest prometheus-cpp avro-cpp pmdk libpmemobj-cpp
    else
        prefix=${THIRD_PARTY_PREFIX} ${THIRD_PARTY_SRC}/prepare.sh build  cmake glog gflags yaml boost zookeeper zlib snappy lz4 jemalloc sparsehash googletest prometheus-cpp avro-cpp
    fi
    if [ "${USE_RDMA}" == "1" ];then
        prefix=${THIRD_PARTY_PREFIX} ${THIRD_PARTY_SRC}/prepare.sh build rdma-core
    fi
}

function build() {
    if [ "${USE_RDMA}" == "1" ]; then
        EXTRA_DEFINE="${EXTRA_DEFINE} -DUSE_RDMA=ON"
    else
        EXTRA_DEFINE="${EXTRA_DEFINE} -DUSE_RDMA=OFF"
    fi
    if [ "${USE_DCPMM}" == "1" ]; then
        EXTRA_DEFINE="${EXTRA_DEFINE} -DUSE_DCPMM=ON"
    else
        EXTRA_DEFINE="${EXTRA_DEFINE} -DUSE_DCPMM=OFF"
    fi
    if [ "${SKIP_BUILD_TEST}" == "1" ]; then
        EXTRA_DEFINE="${EXTRA_DEFINE} -DSKIP_BUILD_TEST=ON"
    else
        EXTRA_DEFINE="${EXTRA_DEFINE} -DSKIP_BUILD_TEST=OFF"
    fi
    mkdir -p ${PROJECT_ROOT}/build
    pushd ${PROJECT_ROOT}/build

    ${THIRD_PARTY_PREFIX}/bin/cmake \
            -DPICO_PS_VERSION:STRING=${PICO_PS_VERSION} \
            -DPICO_PS_VERSION_CODE:STRING=${PICO_PS_VERSION_CODE} \
            -DPICO_CORE_SRC=${PICO_CORE_SRC} \
            -DTHIRD_PARTY=${THIRD_PARTY_PREFIX} \
            -DCMAKE_INSTALL_PREFIX=${prefix} \
                ${EXTRA_DEFINE} ../pico-ps
    if [ 0"${J}" == "0" ];then
        J=`nproc | awk '{print int(($0 + 1)/ 2)}'` # make cocurrent thread number
    fi
    make -j${J} $*

    if [ 0"${prefix}" != "0" ]; then
        make install
    fi
    popd
}

function clean() {
    rm -r -f ${PROJECT_ROOT}/build
    rm -r -f ${PROJECT_ROOT}/output

    if [ 0"${PICO_CORE_SRC}" == "0" ]; then
        pushd ${PROJECT_ROOT}/pico-core
        bash build.sh clean
        popd
    fi
}

function ut() {
    rm -rf ${PROJECT_ROOT}/.ut
    mkdir -p ${PROJECT_ROOT}/.ut
    pushd ${PROJECT_ROOT}/.ut
    #cp -r ${THIRD_PARTY_PREFIX}/zookeeper .
    tests=`find ${PROJECT_ROOT}/build/ -type f -executable -path *_test`
    for i in $tests; do
        echo "[`date +'%Y-%m-%d %H:%M:%S'`] running $i"
        timeout 60m $i > ${PROJECT_ROOT}/.ut/stdout_`basename ${i}` 2> ${PROJECT_ROOT}/.ut/stderr_`basename ${i}`
        echo "[`date +'%Y-%m-%d-%H:%M:%S'`] Success!"
    done
    #tests=`find ${PROJECT_ROOT}/build/ -path *_test`
    popd
    rm -rf ${PROJECT_ROOT}/.ut 
}

function publish_check() {
    git_diff=`git diff`
    if [ "$git_diff" = "" ]; then
        echo "git nodiff"
        return 0
    else
        echo "Please commit your change before publish"
        return 1
    fi
}

function generate_version_code() {
    git_commit_id="`git describe --abbrev=100 --always`"
    export PICO_PS_VERSION_CODE="${PICO_PS_VERSION}-${git_commit_id}"
}

function publish() {
    if [ 0"${prefix}" == "0" ]; then
        echo "no prefix, stop publish."
        return 1
    fi
    pushd ${PROJECT_ROOT}/build
    make install
    popd
}

case "$1" in
    ut)
        ut
    ;;
    clean)
        clean
    ;;
    doc)
        doc_setup
        doc
    ;;
    build|"")
        setup
        build $*
        if [ 0"${prefix}" != "0" ]; then
            publish
        fi
    ;;
    publish)
        publish_check
        generate_version_code
        clean
        setup
        build
        publish
    ;;
    *)
        echo "unkown cmd"
        return 1
    ;;
esac

