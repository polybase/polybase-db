# Build binary
FROM rust:1-bookworm AS builder

# RUN apk add --no-cache gcc musl-dev pkgconf openssl-dev protobuf-dev && apk add --no-cache rust cargo
RUN rustup component add rustfmt
RUN apt update && apt install -y libglib2.0-dev libssl-dev libclang-dev python3 protobuf-compiler libprotobuf-dev

# Set `SYSROOT` to a dummy path (default is /usr) because pkg-config-rs *always*
# links those located in that path dynamically but we want static linking, c.f.
# https://github.com/rust-lang/pkg-config-rs/blob/54325785816695df031cef3b26b6a9a203bbc01b/src/lib.rs#L613
ENV SYSROOT=/dummy
# ENV V8_FROM_SOURC=1

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

RUN mkdir polybase/src && echo "fn main() {}" > polybase/src/main.rs
RUN mkdir indexer/src && echo "fn lib() {}" > indexer/src/lib.rs
RUN mkdir gateway/src && echo "fn lib() {}" > gateway/src/lib.rs
RUN mkdir rbmerkle/src && echo "fn lib() {}" > rbmerkle/src/lib.rs
RUN mkdir migrate/src && echo "fn main() {}" > migrate/src/lib.rs

RUN cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

RUN rm target -r

COPY polybase ./polybase/
COPY indexer ./indexer/
COPY gateway ./gateway/
COPY rbmerkle ./rbmerkle/
COPY migrate ./migrate/

RUN cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

# Run unit tests
FROM builder as unit-tests

CMD cargo test


## Client e2e tests (pdolybase/client)
# FROM runtime as e2e-client-tests

# RUN apk add --update nodejs yarn git python3 build-base

# COPY --from=builder-e2e /build/client.test /usr/bin/
# COPY --from=builder-tendermint /build/tendermint /usr/bin/
# COPY --from=builder /build/polybase /usr/bin/

# WORKDIR /build

# ADD "https://api.github.com/repos/polybase/polybase-ts/commits?per_page=1" /latest_commit
# RUN git clone --depth=1 https://github.com/polybase/polybase-ts.git .

# RUN yarn install
# RUN yarn build

# ENV POLYBASE_BINARY /usr/bin/polybase

# CMD ["/usr/bin/client.test", "-test.v"]


# Main
FROM debian:bookworm-slim
# FROM alpine:3.15

ENV ROOT_DIR /polybase
WORKDIR $ROOT_DIR

USER root

RUN groupadd --system spaceman && \
    useradd --system --gid spaceman --home "$ROOT_DIR" spaceman && \
    chown -R spaceman:spaceman "$ROOT_DIR"

# RUN addgroup -S spaceman && \
#     adduser -S -G spaceman spaceman -h "$ROOT_DIR"

RUN apt update && apt install -y curl nano

USER spaceman

COPY --from=builder /build/target/release/polybase /usr/bin/polybase
COPY --from=builder /build/target/release/migrate /usr/bin/polybase-migrate

STOPSIGNAL SIGTERM

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=5s --retries=3 CMD \
    curl -f http://localhost:8080/v0/health || exit 1

ENTRYPOINT ["/usr/bin/polybase"]

VOLUME [ "$ROOT_DIR" ]
