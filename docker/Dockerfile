# Build binary
FROM rust:1-bookworm AS workspace

RUN rustup component add rustfmt && \
    apt update && apt install -y libglib2.0-dev libssl-dev libclang-dev python3 protobuf-compiler libprotobuf-dev

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install cargo-chef --version ^0.1

# Set `SYSROOT` to a dummy path (default is /usr) because pkg-config-rs *always*
# links those located in that path dynamically but we want static linking, c.f.
# https://github.com/rust-lang/pkg-config-rs/blob/54325785816695df031cef3b26b6a9a203bbc01b/src/lib.rs#L613
ENV SYSROOT=/dummy

WORKDIR /build



# Plan the build
FROM workspace AS planner

COPY .cargo/config.toml .cargo/config.toml
COPY Cargo.lock ./
COPY Cargo.toml ./
COPY polybase ./polybase/
COPY indexer ./indexer/
COPY gateway ./gateway/
COPY solid ./solid/
COPY prover ./prover/
RUN cargo chef prepare --recipe-path /recipe.json



# Build binary
FROM workspace AS builder

ARG RELEASE=1

COPY --from=planner /recipe.json /recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo chef cook --recipe-path /recipe.json $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

COPY .cargo/config.toml .cargo/config.toml
COPY Cargo.lock ./
COPY Cargo.toml ./
COPY polybase ./polybase/
COPY indexer ./indexer/
COPY gateway ./gateway/
COPY solid ./solid/
COPY prover ./prover/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

RUN cp /build/target/$(if [ "$RELEASE" = "1" ]; then echo "release"; else echo "debug"; fi)/polybase /build/target


# Main
FROM debian:bookworm-slim

ENV ROOT_DIR /polybase
WORKDIR $ROOT_DIR

USER root

RUN groupadd --system spaceman && \
    useradd --system --gid spaceman --home "$ROOT_DIR" spaceman && \
    chown -R spaceman:spaceman "$ROOT_DIR"

RUN apt update && apt install -y curl nano

USER spaceman

COPY --from=builder /build/target/polybase /usr/bin/polybase

STOPSIGNAL SIGTERM

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=5s --retries=3 CMD \
    curl -f http://localhost:8080/v0/health || exit 1

ENTRYPOINT ["/usr/bin/polybase"]

VOLUME [ "$ROOT_DIR" ]
