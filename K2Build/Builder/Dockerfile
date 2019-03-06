ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2builder:latest
ARG USE_PROXY
RUN proxy apt install -y gcc-8 g++-8 gdb valgrind python3-distutils stow ninja-build protobuf-compiler ragel
RUN proxy apt install -y libboost-all-dev libssl-dev  libc-ares-dev libcrypto++-dev liblz4-dev libgnutls28-dev libprotobuf-dev libsctp-dev libyaml-cpp-dev xfslibs-dev systemtap-sdt-dev

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 700 --slave /usr/bin/g++ g++ /usr/bin/g++-7
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8
# update-alternatives can now be used to set different gcc version, e.g.:
# The default is the last one we installed above.
#RUN update-alternatives --set gcc /usr/bin/gcc-7

RUN proxy apt update && apt-get -y install libdpdk-k2-dev
