#!/usr/bin/env bash
# Cross-compiles rust-uma on macOS (x86_64-unknown-linux-musl, no Docker) and
# ships it to the production host over SSH, then restarts the systemd unit.
#
# Usage:
#   deploy/deploy.sh              # full gate (fmt+clippy+test) then deploy
#   deploy/deploy.sh --skip-checks  # skip the gate (only for an already-verified hotfix)
#
# One-time host prerequisites (see docs/WORKFLOW.md "首次搭建"):
#   rustup target add x86_64-unknown-linux-musl
#   brew install messense/macos-cross-toolchains/x86_64-unknown-linux-musl
#   SSH access to ubuntu@$DEPLOY_HOST with passwordless sudo
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-43.131.1.194}"
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"
REMOTE="${DEPLOY_USER}@${DEPLOY_HOST}"
TARGET="x86_64-unknown-linux-musl"
BIN_NAME="rust-uma"
REMOTE_DIR="/opt/rust-uma"
REMOTE_ENV_DIR="/etc/rust-uma"
REMOTE_ENV_FILE="${REMOTE_ENV_DIR}/rust-uma.env"
REMOTE_UNIT="/etc/systemd/system/rust-uma.service"
SERVICE="rust-uma.service"

cd "$(dirname "$0")/.."

if [[ "${1:-}" != "--skip-checks" ]]; then
  echo "==> verification gate: cargo fmt --check"
  cargo fmt --check
  echo "==> verification gate: cargo clippy --all-targets --all-features -- -D warnings"
  cargo clippy --all-targets --all-features -- -D warnings
  echo "==> verification gate: cargo test"
  cargo test
else
  echo "!! --skip-checks: SKIPPING fmt/clippy/test — only use this for a hotfix already verified this session"
fi

echo "==> cross-compiling release for ${TARGET}"
cargo build --release --target "${TARGET}"
BIN_PATH="target/${TARGET}/release/${BIN_NAME}"
[[ -x "${BIN_PATH}" ]] || { echo "build did not produce ${BIN_PATH}" >&2; exit 1; }

DIRTY=""
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then DIRTY="-dirty"; fi
VERSION_TAG="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)${DIRTY}"
if [[ "${VERSION_TAG}" == *-dirty ]]; then
  echo "!! working tree has uncommitted changes — deploying ${VERSION_TAG} anyway, but this build is NOT reproducible from git history."
fi
echo "==> deploying ${VERSION_TAG} to ${REMOTE}"

echo "==> ensuring remote layout exists"
ssh "${REMOTE}" "sudo mkdir -p ${REMOTE_DIR} ${REMOTE_ENV_DIR} && sudo chown ${DEPLOY_USER}:${DEPLOY_USER} ${REMOTE_DIR}"

if ! ssh "${REMOTE}" "[[ -f ${REMOTE_ENV_FILE} ]]"; then
  echo "==> no remote env file found; bootstrapping ${REMOTE_ENV_FILE} from local .env"
  echo "    (API_ADDR forced to 0.0.0.0:8011, DATA_DIR forced to /var/lib/rust-uma for the server)"
  [[ -f .env ]] || { echo "local .env not found; create ${REMOTE_ENV_FILE} manually on the server first" >&2; exit 1; }
  {
    echo "API_ADDR=0.0.0.0:8011"
    echo "DATA_DIR=/var/lib/rust-uma"
    grep -v -E '^(API_ADDR|DATA_DIR)=' .env
  } | ssh "${REMOTE}" "sudo tee ${REMOTE_ENV_FILE} >/dev/null && sudo chmod 600 ${REMOTE_ENV_FILE} && sudo chown root:root ${REMOTE_ENV_FILE}"
else
  echo "==> remote env file already exists; leaving it untouched"
  echo "    (edit it directly on the server to change RPC endpoints / WSS_RPC_LIST, then: sudo systemctl restart ${SERVICE})"
fi

echo "==> installing systemd unit"
ssh "${REMOTE}" "sudo tee ${REMOTE_UNIT} >/dev/null" < deploy/rust-uma.service
ssh "${REMOTE}" "sudo systemctl daemon-reload"

echo "==> shipping binary"
scp "${BIN_PATH}" "${REMOTE}:${REMOTE_DIR}/rust-uma.new"
ssh "${REMOTE}" "
  set -e
  if [[ -f ${REMOTE_DIR}/rust-uma ]]; then
    cp ${REMOTE_DIR}/rust-uma ${REMOTE_DIR}/rust-uma.prev
  fi
  chmod +x ${REMOTE_DIR}/rust-uma.new
  mv ${REMOTE_DIR}/rust-uma.new ${REMOTE_DIR}/rust-uma
"

echo "==> restarting ${SERVICE}"
ssh "${REMOTE}" "sudo systemctl enable --now ${SERVICE} >/dev/null 2>&1; sudo systemctl restart ${SERVICE}"

echo "==> waiting for health check (initial Gamma catalog sync can take a while on a cold data dir)"
HEALTHY=""
for i in $(seq 1 60); do
  if ssh "${REMOTE}" "curl -fsS http://127.0.0.1:8011/healthz" > /tmp/rust_uma_health.json 2>/dev/null; then
    HEALTHY=1
    break
  fi
  sleep 3
done

if [[ -n "${HEALTHY}" ]]; then
  echo "==> healthy:"
  cat /tmp/rust_uma_health.json
  echo
  echo "==> deployed ${VERSION_TAG}"
  exit 0
fi

echo "!! service did not answer /healthz within 180s; recent logs:" >&2
ssh "${REMOTE}" "sudo systemctl status ${SERVICE} --no-pager -l; echo; sudo journalctl -u ${SERVICE} -n 80 --no-pager" >&2
echo "!! run deploy/rollback.sh to restore the previous binary" >&2
exit 1
