FROM ghcr.io/faasm/faabric-base:0.22.0
ARG FAABRIC_VERSION

# Flag to say we're in a container
ENV FAABRIC_DOCKER="on"
SHELL ["/bin/bash", "-c"]

# Put the code in place
RUN rm -rf /code \
    && mkdir -p /code/faabric \
    && git clone \
        -b v${FAABRIC_VERSION} \
        https://github.com/faasm/faabric \
        /code/faabric \
    && cd /code/faabric \
    && ./bin/create_venv.sh \
    && source venv/bin/activate \
    && inv dev.conan --build=Release \
    && inv dev.cmake --build=Release \
    && inv dev.cc planner_server

ENTRYPOINT ["/code/faabric/bin/planner_entrypoint.sh"]
