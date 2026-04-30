#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-/www/CareerPilotDJY}"
SERVICE_NAME="${2:-careerpilot}"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"

cat > "${SERVICE_PATH}" <<EOF
[Unit]
Description=CareerPilot Streamlit Service
After=network.target

[Service]
Type=simple
WorkingDirectory=${PROJECT_DIR}
EnvironmentFile=-${PROJECT_DIR}/.env
ExecStart=${PROJECT_DIR}/.venv_server/bin/python ${PROJECT_DIR}/serve.py
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
systemctl restart "${SERVICE_NAME}.service"
systemctl --no-pager --full status "${SERVICE_NAME}.service" | sed -n '1,20p'
