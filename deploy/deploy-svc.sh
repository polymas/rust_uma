#!/usr/bin/env bash
# 部署 uma-console / uma-edge（与 deploy.sh 同一套：Mac 交叉编译 musl，scp，systemd）。
#
# Usage:
#   deploy/deploy-svc.sh uma-console <host> [--skip-checks]
#   deploy/deploy-svc.sh uma-edge    <host> [--skip-checks]
#
# 首次部署会用本地 deploy/<bin>.env.example 引导远端 env 文件（token 留空则启动失败，
# 需要 ssh 上去填好再 restart）；已存在则不动。
set -euo pipefail

BIN_NAME="${1:?usage: deploy-svc.sh <uma-console|uma-edge> <host> [--skip-checks]}"
DEPLOY_HOST="${2:?usage: deploy-svc.sh <uma-console|uma-edge> <host> [--skip-checks]}"
SKIP="${3:-}"
case "${BIN_NAME}" in
  uma-console) PORT=8013 ;;
  uma-edge) PORT=8012 ;;
  *) echo "unknown binary ${BIN_NAME}" >&2; exit 1 ;;
esac
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"
REMOTE="${DEPLOY_USER}@${DEPLOY_HOST}"
TARGET="x86_64-unknown-linux-musl"
REMOTE_DIR="/opt/${BIN_NAME}"
REMOTE_ENV_DIR="/etc/${BIN_NAME}"
REMOTE_ENV_FILE="${REMOTE_ENV_DIR}/${BIN_NAME}.env"
REMOTE_UNIT="/etc/systemd/system/${BIN_NAME}.service"
SERVICE="${BIN_NAME}.service"
HEALTH_PATH="/healthz"
[[ "${BIN_NAME}" == "uma-edge" ]] && HEALTH_PATH="/edge/healthz"

cd "$(dirname "$0")/.."

if [[ "${SKIP}" != "--skip-checks" ]]; then
  echo "==> verification gate: fmt / clippy / test"
  cargo fmt --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
else
  echo "!! --skip-checks: SKIPPING fmt/clippy/test"
fi

echo "==> cross-compiling ${BIN_NAME} for ${TARGET}"
cargo build --release --target "${TARGET}" --bin "${BIN_NAME}"
BIN_PATH="target/${TARGET}/release/${BIN_NAME}"
[[ -x "${BIN_PATH}" ]] || { echo "build did not produce ${BIN_PATH}" >&2; exit 1; }

DIRTY=""
[[ -n "$(git status --porcelain 2>/dev/null)" ]] && DIRTY="-dirty"
VERSION_TAG="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)${DIRTY}"
echo "==> deploying ${BIN_NAME} ${VERSION_TAG} to ${REMOTE}"

ssh "${REMOTE}" "sudo mkdir -p ${REMOTE_DIR} ${REMOTE_ENV_DIR} && sudo chown ${DEPLOY_USER}:${DEPLOY_USER} ${REMOTE_DIR}"

if ! ssh "${REMOTE}" "[[ -f ${REMOTE_ENV_FILE} ]]"; then
  echo "==> bootstrapping ${REMOTE_ENV_FILE} from deploy/${BIN_NAME}.env.example"
  ssh "${REMOTE}" "sudo tee ${REMOTE_ENV_FILE} >/dev/null && sudo chmod 600 ${REMOTE_ENV_FILE} && sudo chown root:root ${REMOTE_ENV_FILE}" < "deploy/${BIN_NAME}.env.example"
  echo "!! 远端 env 是模板，token 为空服务会拒绝启动；填好后: sudo systemctl restart ${SERVICE}"
else
  echo "==> remote env file exists; leaving it untouched"
fi

echo "==> installing systemd unit"
ssh "${REMOTE}" "sudo tee ${REMOTE_UNIT} >/dev/null" < "deploy/${BIN_NAME}.service"
ssh "${REMOTE}" "sudo systemctl daemon-reload"

echo "==> shipping binary"
scp "${BIN_PATH}" "${REMOTE}:${REMOTE_DIR}/${BIN_NAME}.new"
ssh "${REMOTE}" "
  set -e
  if [[ -f ${REMOTE_DIR}/${BIN_NAME} ]]; then cp ${REMOTE_DIR}/${BIN_NAME} ${REMOTE_DIR}/${BIN_NAME}.prev; fi
  chmod +x ${REMOTE_DIR}/${BIN_NAME}.new
  mv ${REMOTE_DIR}/${BIN_NAME}.new ${REMOTE_DIR}/${BIN_NAME}
"

echo "==> restarting ${SERVICE}"
ssh "${REMOTE}" "sudo systemctl enable --now ${SERVICE} >/dev/null 2>&1; sudo systemctl restart ${SERVICE}"

for i in $(seq 1 20); do
  if ssh "${REMOTE}" "curl -fsS http://127.0.0.1:${PORT}${HEALTH_PATH}" 2>/dev/null; then
    echo; echo "==> deployed ${BIN_NAME} ${VERSION_TAG}"; exit 0
  fi
  sleep 2
done
echo "!! ${SERVICE} did not answer ${HEALTH_PATH} within 40s; recent logs:" >&2
ssh "${REMOTE}" "sudo systemctl status ${SERVICE} --no-pager -l; echo; sudo journalctl -u ${SERVICE} -n 60 --no-pager" >&2
echo "!! rollback: ssh ${REMOTE} 'mv ${REMOTE_DIR}/${BIN_NAME}.prev ${REMOTE_DIR}/${BIN_NAME} && sudo systemctl restart ${SERVICE}'" >&2
exit 1
