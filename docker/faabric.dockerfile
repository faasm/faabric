FROM faasm.azurecr.io/faabric-base:0.21.0
ARG FAABRIC_VERSION

# faabic-base image is not re-built often, so tag may be behind
SHELL ["/bin/bash", "-c"]

# Flag to say we're in a container
ENV FAABRIC_DOCKER="on"

# Put the code in place
WORKDIR /code
RUN git clone \
        -b v${FAABRIC_VERSION} https://github.com/faasm/faabric \
    && git config --global --add safe.directory /code/faabric

WORKDIR /code/faabric

# Python set-up and code builds
RUN ./bin/create_venv.sh \
    && source venv/bin/activate \
    # Static build
    && inv dev.cmake --build=Release \
    && inv dev.cc faabric \
    # Shared build
    && inv dev.cmake --shared --build=Release \
    && inv dev.cc faabric --shared \
    && inv dev.install faabric --shared

# GDB config, allow loading repo-specific config
RUN echo "set auto-load safe-path /" > /root/.gdbinit

# CLI setup
ENV TERM=xterm-256color

RUN echo ". /code/faabric/bin/workon.sh" >> ~/.bashrc
CMD ["/bin/bash", "-l"]
