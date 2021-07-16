FROM 4pdosc/prpc-base:0.1.0

ADD . /parameter-server
WORKDIR /parameter-server/pico-core
RUN mkdir -p build && cd build && cmake .. && make -j$J -DSKIP_BUILD_TEST=ON && make install && cd ..
WORKDIR /parameter-server
RUN mkdir -p build && cd build && cmake .. && make -j$J && make test && make install && cd ..
