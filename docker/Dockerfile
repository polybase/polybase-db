# Build binary
FROM rust:1-bookworm AS builder

RUN rustup component add rustfmt && \
    apt update && apt install -y libglib2.0-dev libssl-dev libclang-dev python3 protobuf-compiler libprotobuf-dev

# Set `SYSROOT` to a dummy path (default is /usr) because pkg-config-rs *always*
# links those located in that path dynamically but we want static linking, c.f.
# https://github.com/rust-lang/pkg-config-rs/blob/54325785816695df031cef3b26b6a9a203bbc01b/src/lib.rs#L613
ENV SYSROOT=/dummy

ARG RELEASE=1

WORKDIR /build

COPY Cargo.lock ./
COPY Cargo.toml ./
COPY polybase/Cargo.toml ./polybase/
COPY indexer/Cargo.toml ./indexer/
COPY gateway/Cargo.toml ./gateway/
COPY rbmerkle/Cargo.toml ./rbmerkle/
COPY rbmerkle/benches ./rbmerkle/benches
COPY migrate/Cargo.toml ./migrate/

RUN mkdir polybase/src && echo "fn main() {}" > polybase/src/main.rs && \
    mkdir indexer/src && echo "fn lib() {}" > indexer/src/lib.rs && \
    mkdir gateway/src && echo "fn lib() {}" > gateway/src/lib.rs && \
    mkdir rbmerkle/src && echo "fn lib() {}" > rbmerkle/src/lib.rs && \
    mkdir migrate/src && echo "fn main() {}" > migrate/src/lib.rs && \
    cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi) && \
    rm target -r

COPY polybase ./polybase/
COPY indexer ./indexer/
COPY gateway ./gateway/
COPY rbmerkle ./rbmerkle/
COPY migrate ./migrate/

RUN cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

RUN cp /build/target/$(if [ "$RELEASE" = "1" ]; then echo "release"; else echo "debug"; fi)/polybase /build/target && \
    cp /build/target/$(if [ "$RELEASE" = "1" ]; then echo "release"; else echo "debug"; fi)/migrate /build/target

# Run unit tests
FROM builder as unit-tests

CMD cargo test


## polybase-ts e2e tests
FROM builder as e2e-polybase-ts-tests

RUN curl -fsSL https://deb.nodesource.com/setup_current.x | bash - && \
    apt-get install -y nodejs git build-essential && npm install -g yarn

COPY --from=builder /build/target/polybase /usr/bin/

WORKDIR /e2e

ADD "https://api.github.com/repos/polybase/polybase-ts/commits?per_page=1" /latest_commit
RUN git clone --depth=1 https://github.com/polybase/polybase-ts.git
RUN cd polybase-ts && yarn install && yarn build

CMD ["bash", "-c", "polybase & p1=$! && cd polybase-ts/packages/client && yarn test:e2e && kill \"$p1\" && echo \"Tests passed\""]


# Main
FROM debian:bookworm-slim
# FROM alpine:3.15

ENV ROOT_DIR /polybase
WORKDIR $ROOT_DIR

USER root

RUN groupadd --system spaceman && \
    useradd --system --gid spaceman --home "$ROOT_DIR" spaceman && \
    chown -R spaceman:spaceman "$ROOT_DIR"

RUN apt update && apt install -y curl nano

USER spaceman

COPY --from=builder /build/target/polybase /usr/bin/polybase
COPY --from=builder /build/target/migrate /usr/bin/polybase-migrate

STOPSIGNAL SIGTERM

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=5s --retries=3 CMD \
    curl -f http://localhost:8080/v0/health || exit 1

ENTRYPOINT ["/usr/bin/polybase"]

VOLUME [ "$ROOT_DIR" ]
