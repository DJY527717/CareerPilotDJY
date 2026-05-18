#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-${PROJECT_DIR}/.venv_server}"
SERVICE_NAME="${SERVICE_NAME:-careerpilot}"

cd "${PROJECT_DIR}"

echo "[deploy] project dir: ${PROJECT_DIR}"

if [[ -f "${PROJECT_DIR}/.env" ]]; then
  echo "[deploy] loading env file"
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_DIR}/.env"
  set +a
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "[deploy] creating virtualenv at ${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

echo "[deploy] upgrading pip"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip

echo "[deploy] installing requirements"
"${VENV_DIR}/bin/pip" install -r requirements.txt

if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files | grep -q "^${SERVICE_NAME}\.service"; then
  echo "[deploy] restarting systemd service ${SERVICE_NAME}.service"
  systemctl daemon-reload
  systemctl restart "${SERVICE_NAME}.service"
  systemctl --no-pager --full status "${SERVICE_NAME}.service" | sed -n '1,20p'
else
  echo "[deploy] systemd service not found, using nohup fallback"
  pkill -f "python .*serve.py" || true
  pkill -f "streamlit run app.py" || true
  nohup "${VENV_DIR}/bin/python" serve.py > careerpilot.log 2>&1 &
  sleep 3
  tail -n 40 careerpilot.log || true
fi

HEALTHCHECK_URL="${HEALTHCHECK_URL:-http://127.0.0.1:${APP_PORT:-8000}/api/health}"
echo "[deploy] waiting for health check: ${HEALTHCHECK_URL}"
if command -v curl >/dev/null 2>&1; then
  for attempt in {1..20}; do
    if curl -fsS "${HEALTHCHECK_URL}" >/dev/null; then
      echo "[deploy] health check passed"
      break
    fi
    if [[ "${attempt}" -eq 20 ]]; then
      echo "[deploy] health check failed"
      exit 1
    fi
    sleep 2
  done
else
  echo "[deploy] curl not found, skipping HTTP health check"
fi

echo "[deploy] done"
