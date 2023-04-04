FROM faasm.azurecr.io/faabric-base:0.4.2
ARG FAABRIC_VERSION

# Flag to say we're in a container
ENV FAABRIC_DOCKER="on"

# Put the code in place
WORKDIR /code
RUN rm -rf /code \
    && mkdir -p /code \
    && git clone \
        -b v${FAABRIC_VERSION} \
        https://github.com/faasm/faabric \
        /code/faabric \
    && cd /code/faabric \
    && ./bin/create_venv.sh \
    && source venv/bin/activate \
    && inv dev.cmake --build=Release \
    && inv dev.cc planner_server

ENTRYPOINT ["/build/faabric/static/bin/planner_server"]
