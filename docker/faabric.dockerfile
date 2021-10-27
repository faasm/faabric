FROM faasm/faabric-base:0.2.0
ARG FAABRIC_VERSION

# faabic-base image is not re-built often, so tag may be behind

# Flag to say we're in a container
ENV FAABRIC_DOCKER="on"

# Put the code in place
WORKDIR /code
RUN git clone -b v${FAABRIC_VERSION} https://github.com/faasm/faabric

WORKDIR /code/faabric
RUN pip3 install -r requirements.txt

# Static build
RUN inv dev.cmake --build=Release
RUN inv dev.cc faabric
RUN inv dev.cc faabric_tests

# Shared build
RUN inv dev.cmake --shared --build=Release
RUN inv dev.cc faabric --shared
RUN inv dev.install faabric --shared

# CLI setup
ENV TERM xterm-256color
SHELL ["/bin/bash", "-c"]

RUN echo ". /code/faabric/bin/workon.sh" >> ~/.bashrc
CMD ["/bin/bash", "-l"]
