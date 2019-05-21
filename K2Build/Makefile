include DFV_K2_BuildDeps/DockerMake.base

package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build
INSTALL_PFX=install/usr/local
BUILD_DIR=build/release
#DEPEND_FLAGS=--depends libfmt-k2 -d 'libdpdk-k2-dev = 0.3.1' -d libboost-thread-dev -d libboost-system-dev -d libboost-chrono-dev -d libboost-program-options-dev -d libboost-filesystem-dev -d libboost-test-dev -d liblz4-dev -d libgnutls28-dev -d libprotobuf-dev -d libyaml-cpp-dev -d libc-ares-dev -d libcrypto++-dev -d libsctp-dev -d libunistring-dev
DEPEND_FLAGS=--depends libfmt-k2 -d libboost-thread-dev -d libboost-system-dev -d libboost-chrono-dev -d libboost-program-options-dev -d libboost-filesystem-dev -d libboost-test-dev -d liblz4-dev -d libgnutls28-dev -d libprotobuf-dev -d libyaml-cpp-dev -d libc-ares-dev -d libcrypto++-dev -d libsctp-dev -d libunistring-dev -d libibverbs-dev

build: configure
	@echo ">>> Building"
	@cd ../ && ninja -C $(BUILD_DIR)

configure: submodule
	@echo ">>> Configuring"
	@cd ../ && ./configure.py --mode=release --prefix=$(INSTALL_PFX)

submodule:
	@echo ">>> Fetching submodules"
	@cd ../ && git submodule update --init

test: build
	@echo ">>> Testing"
	@cd ../ && ninja -C $(BUILD_DIR) test

install: build
	@echo ">>> Installing"
	@cd ../ && ninja -C $(BUILD_DIR) install

clean:
	@echo ">>> Cleaning"
	@cd ../ && ninja -C $(BUILD_DIR) clean || true
	@rm -f libseastar-k2_* || true
	@cd ../ && rm -rf build

package: install
	@echo ">>> Packaging"
	@cd ../ && fpm $(DEPEND_FLAGS) -s dir -t deb --deb-generate-changes -n libseastar-k2 -f -m k2 -a amd64 -v $(package_version) --deb-dist stable --description "Seastar k2 package http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_SeaStar/" -C $(BUILD_DIR)/install
	@cd ../ && fpm -s dir -t rpm -n libseastar-k2 -f -m k2 -a amd64 -v $(package_version) --rpm-dist stable --description "Seastar k2 package http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_SeaStar/" -C $(BUILD_DIR)/install

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push

