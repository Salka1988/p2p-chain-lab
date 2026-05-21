FROM rust:1.91-slim AS builder

WORKDIR /app
COPY . .
RUN cargo build --release -p p2p-chain-lab-cli

FROM debian:trixie-slim

RUN useradd --create-home --uid 10001 app
COPY --from=builder /app/target/release/p2p-chain-lab-cli /usr/local/bin/p2p-chain-lab-cli
USER app

ENTRYPOINT ["p2p-chain-lab-cli"]
