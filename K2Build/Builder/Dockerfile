ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2builder:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y python3-distutils stow ninja-build protobuf-compiler ragel libboost-all-dev libssl-dev  libc-ares-dev libcrypto++-dev liblz4-dev libgnutls28-dev libprotobuf-dev libsctp-dev libyaml-cpp-dev xfslibs-dev systemtap-sdt-dev libfmt-k2
RUN proxy apt update && proxy apt-get install -y libdpdk-k2-dev=0.3.1
