ARG FAABRIC_VERSION
FROM faasm/faabric-cli:$FAABRIC_VERSION

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get install -y \
    libpython3-dev \
    python3-dev \
    python3-pip \
    python3-venv

WORKDIR /code/faabric
COPY requirements.txt .
RUN pip3 install -r requirements.txt

