FROM rust:1.91-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release -p heph

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/heph /usr/local/bin/heph

EXPOSE 4024
VOLUME /data

ENTRYPOINT ["heph"]
CMD ["--host", "0.0.0.0", "--data-dir", "/data"]
