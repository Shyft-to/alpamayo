#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="rust-builder"
IMAGE_TAG="latest"
IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
PLATFORM="linux/amd64"

echo "========================================"
echo " Rust Docker Builder"
echo "========================================"

if [ ! -f Dockerfile ]; then
  echo "ERROR: Dockerfile not found in current directory."
  exit 1
fi

NEED_BUILD=0

# Check image existence
if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "[BUILD] Docker image not found."
  NEED_BUILD=1
else
  ARCH=$(docker image inspect "${IMAGE}" --format '{{.Architecture}}')

  if [ "$ARCH" != "amd64" ]; then
    echo "[BUILD] Existing image architecture is '$ARCH', expected 'amd64'."
    NEED_BUILD=1
  else
    echo "[OK] Docker image already exists: ${IMAGE} (${ARCH})"
  fi
fi

# Build image if needed
if [ "$NEED_BUILD" -eq 1 ]; then
  echo "[BUILD] Building ${IMAGE} for ${PLATFORM}..."

  docker build \
    --platform "${PLATFORM}" \
    -t "${IMAGE}" \
    .

  echo "[OK] Docker image built successfully."
fi

echo "[BUILD] Starting cargo build --release..."

docker run --rm \
  --platform "${PLATFORM}" \
  -v "$PWD:/workspace" \
  -v "$PWD/target:/workspace/target" \
  -v "$HOME/.cargo/registry:/usr/local/cargo/registry" \
  -v "$HOME/.cargo/git:/usr/local/cargo/git" \
  -v "$HOME/.rustup:/usr/local/rustup" \
  -w /workspace \
  "${IMAGE}" \
  cargo build --release

echo "========================================"
echo " Build completed successfully"
echo "========================================"
