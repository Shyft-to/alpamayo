docker run --rm \
  --platform linux/amd64 \
  -v "$PWD:/workspace" \
  -v "$HOME/.cargo/registry:/usr/local/cargo/registry" \
  -w /workspace \
  rust:1.93.0 \
  bash -c "apt-get update -qq && apt-get install -y --no-install-recommends clang libclang-dev && cargo build --release"