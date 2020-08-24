FROM faabric/base:0.0.1

# Install Catch
WORKDIR /usr/local/include/catch
RUN wget -q \
    -O catch.hpp \
    https://raw.githubusercontent.com/catchorg/Catch2/master/single_include/catch2/catch.hpp

# Install Redis
RUN apt install -y redis redis-tools

# Build tests
COPY . /code

WORKDIR /code/build
RUN cmake \
  -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  ..

RUN cmake --build . --target faabric_tests

# Test runner
RUN chmod +x /code/docker/run_tests.sh
CMD /code/docker/run_tests.sh

