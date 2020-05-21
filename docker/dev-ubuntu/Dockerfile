FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt upgrade -y && apt install -y \
    apt-transport-https \
    pkg-config \
    ca-certificates \
    cmake \
    curl \
    gdb \
    valgrind \
    software-properties-common \
    vim \
    bash \
    ruby \
    python3 \
    python3-pip \
    ruby-dev \
    rubygems \
    build-essential \
    openjdk-8-jdk \
    rpm \
    scons \
    git \
    sed;

# easy way to get cmake 3.17
RUN pip3 install cmake

VOLUME [ "/build" ]
WORKDIR /build

CMD ["/bin/bash"]
