FROM docker.io/debian:trixie-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake ninja-build git curl zip unzip tar pkg-config \
    build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Blobless clone is much faster than full clone for a pinned checkout
RUN git clone --filter=blob:none https://github.com/microsoft/vcpkg /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg

WORKDIR /build
COPY . .

# VCPKG_BINARY_SOURCES points at the BuildKit cache so compiled ports survive
# layer invalidation. x64-linux-static links ZMQ/Boost/json into the binary —
# runtime stage needs no extra packages.
RUN --mount=type=cache,target=/root/.cache/vcpkg \
    VCPKG_BINARY_SOURCES="clear;files,/root/.cache/vcpkg,readwrite" \
    cmake --preset default \
    && cmake --build build --preset default

FROM docker.io/debian:trixie-slim

COPY --from=builder /build/build/lclstream-fastcache /usr/local/bin/lclstream-fastcache

CMD ["lclstream-fastcache"]
