FROM rust:1.93.0

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
      clang \
      libclang-dev && \
    rm -rf /var/lib/apt/lists/*