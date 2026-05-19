FROM rust:1.91-slim AS builder

WORKDIR /app
COPY . .
RUN cargo build --release -p libp2p-mini-chain-cli

FROM debian:trixie-slim

RUN useradd --create-home --uid 10001 app
COPY --from=builder /app/target/release/libp2p-mini-chain-cli /usr/local/bin/libp2p-mini-chain-cli
USER app

ENTRYPOINT ["libp2p-mini-chain-cli"]
