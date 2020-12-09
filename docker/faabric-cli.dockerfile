ARG FAABRIC_VERSION
FROM faasm/faabric:$FAABRIC_VERSION

SHELL ["/bin/bash", "-c"]

ENV TERM xterm-256color

# Prepare bashrc
RUN echo ". /code/faabric/bin/workon.sh" >> ~/.bashrc
CMD ["/bin/bash", "-l"]
