#!/usr/bin/env bash
# Restores the previous binary saved by the last deploy.sh run (rust-uma.prev)
# and restarts the service. Only one generation of rollback is kept.
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-43.131.1.194}"
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"
REMOTE="${DEPLOY_USER}@${DEPLOY_HOST}"
REMOTE_DIR="/opt/rust-uma"
SERVICE="rust-uma.service"

ssh "${REMOTE}" "
  set -e
  if [[ ! -f ${REMOTE_DIR}/rust-uma.prev ]]; then
    echo 'no ${REMOTE_DIR}/rust-uma.prev found — nothing to roll back to' >&2
    exit 1
  fi
  cp ${REMOTE_DIR}/rust-uma ${REMOTE_DIR}/rust-uma.rolled-back-from
  mv ${REMOTE_DIR}/rust-uma.prev ${REMOTE_DIR}/rust-uma
  sudo systemctl restart ${SERVICE}
"
echo "==> rolled back and restarted ${SERVICE}; waiting for health check"
for i in $(seq 1 20); do
  if ssh "${REMOTE}" "curl -fsS http://127.0.0.1:8011/healthz" 2>/dev/null; then
    echo
    echo "==> healthy after rollback"
    exit 0
  fi
  sleep 3
done
echo "!! still not healthy after rollback; check: ssh ${REMOTE} sudo journalctl -u ${SERVICE} -n 80 --no-pager" >&2
exit 1
