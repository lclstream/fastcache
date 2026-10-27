# syntax=docker/dockerfile:1
# Stage 1: 'base' Base Ubuntu 24 setup

ARG DEPS_VER=0.4.0rc1
ARG E2SAR_DEPS_DEB=E2SAR-${DEPS_VER}-main-ubuntu-24.04/e2sar-deps_${DEPS_VER}_amd64.deb
ARG E2SAR_DEPS_DEB_URL=https://github.com/JeffersonLab/E2SAR/releases/download/${E2SAR_DEPS_DEB}
ARG E2SARINSTALL=/e2sar-install

FROM ubuntu:24.04 AS base

ARG E2SAR_DEPS_DEB_URL
ARG E2SARINSTALL

ENV DISTRO=ubuntu24
ENV HOME=/home/ubuntu
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV PATH=/usr/local/bin:$PATH
ENV E2SARINSTALL=${E2SARINSTALL}

RUN apt-get -yq update && \
    apt-get -yq install --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        autoconf \
        cmake \
        git \
        libtool \
        pkg-config \
        libglib2.0-dev \
        ninja-build \
        openssl \
        libssl-dev \
        libsystemd-dev \
        protobuf-compiler \
        libre2-dev \
        wget \
        ca-certificates && \
    pip3 install --break-system-packages pybind11 meson && \
    wget -q -O boost_grpc.deb ${E2SAR_DEPS_DEB_URL} && \
    dpkg -i ./boost_grpc.deb && \
    rm -f ./boost_grpc.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Stage 2. 'compile-e2sar' Download and compile E2SAR

FROM base AS compile-e2sar

ARG E2SARINSTALL

WORKDIR /src

COPY . /src/

RUN git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git clone --recurse-submodules https://github.com/JeffersonLab/E2SAR.git && cd E2SAR && \
    git checkout 55b7337802f4cf0d1ec5a93d2b38059a5c6bd937 && \
    PATH=$HOME/.local/bin:/usr/local/bin:$PATH BOOST_ROOT=/usr/local/ LD_LIBRARY_PATH=${LD_LIBRARY_PATH} CXXFLAGS="-fpermissive" \
    meson setup --libdir=lib64 -Dpkg_config_path=${PKG_CONFIG_PATH} --prefix ${E2SARINSTALL} build && \
    sed -i 's/-std=c++11//g' build/build.ninja && \
    meson compile -C build -j 2 && \
    meson install -C build


# Stage 3: 'compile-fastcache' Download and build fastcache using vcpkg

FROM compile-e2sar AS compile-fastcache
RUN apt-get -yq update && \
    apt-get -yq install --no-install-recommends \
        git \
        curl \
        zip \
        unzip \
        tar \
        python3 \
        python3-pip \
        liburing-dev && \
    git clone --filter=blob:none https://github.com/microsoft/vcpkg /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics && \
    git clone --branch feature/ejfat_worker --single-branch https://github.com/lclstream/fastcache.git /build
ENV VCPKG_ROOT=/opt/vcpkg
WORKDIR /build

ENV EJFAT_PATH=$E2SARINSTALL
ENV GRPC_PATH=/usr/local
RUN --mount=type=cache,target=/root/.cache/vcpkg \
    VCPKG_BINARY_SOURCES="clear;files,/root/.cache/vcpkg,readwrite" && \
    cmake --preset release \
    && cmake --build build --preset release

# Stage 4: 'runtime' Final runtime setup of env vars and copy runtime libs

FROM ubuntu:24.04 AS runtime
COPY --from=compile-e2sar $E2SARINSTALL $E2SARINSTALL
COPY --from=compile-e2sar /usr/local/lib/libgrpc* /usr/local/lib/
COPY --from=compile-e2sar /usr/local/lib/libprotobuf* /usr/local/lib/
COPY --from=compile-e2sar /usr/local/lib/ /usr/local/lib/
ENV LD_LIBRARY_PATH=/usr/local/lib:$E2SARINSTALL/lib64:$LD_LIBRARY_PATH
ENV PATH=$E2SARINSTALL/bin:$PATH
COPY --from=compile-fastcache /build/build/lclstream-fastcache /usr/local/bin/lclstream-fastcache

CMD ["lclstream-fastcache"]
