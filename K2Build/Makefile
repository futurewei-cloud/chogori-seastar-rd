include DFV_K2_BuildDeps/DockerMake.base

package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build
INSTALL_PFX=install/usr/local
BUILD_DIR=build/release
DEPEND_FLAGS=--depends libfmt-k2 -d libboost-thread-dev -d libboost-system-dev -d libboost-chrono-dev -d libboost-program-options-dev -d libboost-filesystem-dev -d libboost-test-dev -d liblz4-dev -d libgnutls28-dev -d libprotobuf-dev -d libyaml-cpp-dev -d libc-ares-dev -d libcrypto++-dev -d libsctp-dev -d libunistring-dev -d 'libibverbs-dev = 22.1-1' -d pkgconf -d libnuma-dev -d libhwloc-dev

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
	@rm -f ../libseastar-k2* || true
	@cd ../ && rm -rf build

package_deb: install $(wildcard ../libseastar-k2_*.changes) $(wildcard ../libseastar-k2_*.deb)
	@echo ">>> Packaging deb"
	@cd ../ && fpm $(DEPEND_FLAGS) -s dir -t deb --deb-generate-changes -n libseastar-k2 -f -m k2 -a amd64 -v $(package_version) --deb-dist stable --description "Seastar k2 package http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_SeaStar/" -C $(BUILD_DIR)/install

package_rpm: install $(wildcard ../libseastar-k2*.rpm)
	@echo ">>> Packaging rpm"
	@cd ../ && fpm -s dir -t rpm -n libseastar-k2 -f -m k2 -a amd64 -v $(package_version) --rpm-dist stable --description "Seastar k2 package http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_SeaStar/" -C $(BUILD_DIR)/install

package_push_deb: package_deb
	@$(eval DEB_NAME := $(wildcard ../libseastar-k2_*.deb))
	@$(eval DEB_CH_NAME := $(wildcard ../libseastar-k2_*.changes))
	@echo ">>> Pushing package: $(DEB_NAME)"
	@$(shell curl -s http://k2-bvu-10001.huawei.com:10080/incoming/ --upload-file $(DEB_NAME))
	@echo ">>> Pushing package: $(DEB_CH_NAME)"
	@$(shell curl -s http://k2-bvu-10001.huawei.com:10080/incoming/ --upload-file $(DEB_CH_NAME))

package_push_rpm: package_rpm
	@$(eval RPM_NAME := $(wildcard ../libseastar-k2*.rpm))
	@echo ">>> Pushing package: $(RPM_NAME)"
	@$(shell curl -s http://k2-bvu-10001.huawei.com:10070/ --upload-file $(RPM_NAME))
	@echo ">>> Committing pushed packages"
	@$(shell curl -s http://k2-bvu-10001.huawei.com:10070/commit)

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push

