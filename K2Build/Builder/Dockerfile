ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2builder:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y python3-distutils stow ninja-build protobuf-compiler ragel libssl-dev  libc-ares-dev libcrypto++-dev liblz4-dev libgnutls28-dev libprotobuf-dev libsctp-dev libyaml-cpp-dev xfslibs-dev systemtap-sdt-dev libfmt-k2 libboost-thread-dev libboost-system-dev libboost-chrono-dev libboost-program-options-dev libboost-filesystem-dev libboost-test-dev pkgconf libhwloc-dev libunistring-dev
RUN proxy apt update && proxy apt-get install -y libibverbs-dev
