FROM faabric/base:0.0.1

COPY . /code

WORKDIR /code/build
RUN cmake \
  -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  ..
RUN ninja

RUN cmake --build . --target faabric_tests

RUN ./bin/test
