FROM faabric/base:0.0.1

# Install Catch
WORKDIR /usr/local/include/catch
RUN wget -q \
    -O catch.hpp \
    https://raw.githubusercontent.com/catchorg/Catch2/master/single_include/catch2/catch.hpp

# Install Redis
RUN apt install -y redis redis-tools

COPY . /code

WORKDIR /code/build
RUN cmake \
  -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  ..

RUN cmake --build . --target faabric_tests

# Entrypoint
RUN chmod +x /code/docker/test_entrypoint.sh
ENTRYPOINT /code/docker/test_entrypoint.sh

CMD ./bin/faabric_tests
