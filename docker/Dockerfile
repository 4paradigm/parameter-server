FROM 4pdosc/prpc-base:0.1.0

ADD . /parameter-server
WORKDIR /parameter-server/pico-core
RUN J=`nproc | awk '{print int(($0 + 1)/ 2)}'` && \
    mkdir -p build && cd build && cmake -DSKIP_BUILD_TEST=ON .. && make -j$J && make install && cd ..
WORKDIR /parameter-server
RUN J=`nproc | awk '{print int(($0 + 1)/ 2)}'` && \
    mkdir -p build && cd build && cmake .. && make -j$J && make test && make install && cd ..
