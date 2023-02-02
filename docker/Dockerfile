# Build binary
FROM rust:1-alpine3.17 AS builder

RUN apk add --no-cache musl-dev pkgconf

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

RUN mkdir polybase/src && echo "fn main() {}" > polybase/src/main.rs
RUN mkdir indexer/src && echo "fn main() {}" > indexer/src/main.rs

RUN cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

RUN rm target -r

COPY polybase ./polybase/
COPY indexer ./indexer/

RUN cargo build $(if [ "$RELEASE" = "1" ]; then echo "--release"; fi)

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
FROM alpine:3.15

ENV ROOT_DIR /polybase
WORKDIR $ROOT_DIR

USER root

RUN addgroup -S spaceman && \
    adduser -S -G spaceman spaceman -h "$ROOT_DIR"

RUN apk add --no-cache curl nano

USER spaceman

COPY --from=builder /build/target/release/polybase /usr/bin/polybase

STOPSIGNAL SIGTERM

EXPOSE 8080

# HEALTHCHECK --interval=5s --timeout=5s --retries=3 CMD \
#     curl -f http://localhost:8080/v0/health || exit 1

ENTRYPOINT ["/usr/bin/polybase"]

VOLUME [ "$ROOT_DIR" ]
