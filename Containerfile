# syntax=docker/dockerfile:1.7
#
# Local build (compiles from source):
#   podman build -t coresplitter:dev .
#
# CI build (binary pre-compiled into artifacts/<arch>/, arch = amd64|arm64 to
# match Docker's TARGETARCH; buildx picks the right one per platform):
#   podman build --build-arg BUILDER=prebuilt .
#
# Either way the result is a static musl binary on top of a minimal alpine,
# with no glibc or OpenSSL runtime dependency: SQLite is bundled.

# ARG before the first FROM so it is a global build argument. Default 'builder'
# (local compile); CI passes --build-arg BUILDER=prebuilt to use artifacts/.
ARG BUILDER=builder

# Base-image versions. Defaults mirror versions.env (the single source of
# truth); CI overrides them with --build-arg.
ARG ALPINE_VERSION=3.23
ARG RUST_VERSION=1

# ── Stage 1a: compile from source (default local path) ──────────────────────

FROM docker.io/rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder

# musl-dev pulls in the C toolchain bits libsqlite3-sys needs to bundle
# SQLite; the rest of the stack is pure Rust.
# NOTE: add `dbus-dev` here if building with --features ble.
RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/coresplitter /usr/bin/coresplitter && \
    strip /usr/bin/coresplitter

# ── Stage 1b: pre-built binary injected from CI (artifacts/ in context) ──────

# TARGETARCH (amd64/arm64) is set automatically by buildx per target platform;
# re-declare it here to use it in COPY. The artifacts/<arch>/ layout is produced
# by the container.yml build matrix.
FROM scratch AS prebuilt
ARG TARGETARCH
COPY --chmod=0755 artifacts/${TARGETARCH}/coresplitter /usr/bin/coresplitter

# ── Stage 1c: indirection — points to 'builder' or 'prebuilt' via build arg ──

FROM ${BUILDER} AS bins

# ── Stage 2: runtime ────────────────────────────────────────────────────────

FROM docker.io/alpine:${ALPINE_VERSION} AS runtime

RUN addgroup -S -g 10001 coresplitter && \
    adduser -S -G coresplitter -u 10001 coresplitter && \
    mkdir -p /etc/coresplitter /var/lib/coresplitter && \
    chown -R coresplitter:coresplitter /var/lib/coresplitter

COPY --from=bins /usr/bin/coresplitter /usr/bin/coresplitter

USER coresplitter
WORKDIR /var/lib/coresplitter

EXPOSE 5000

ENV CORESPLITTER_DATA_DIR=/var/lib/coresplitter

ENTRYPOINT ["/usr/bin/coresplitter"]
