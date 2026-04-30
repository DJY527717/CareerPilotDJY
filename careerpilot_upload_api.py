from __future__ import annotations

import json
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app import (
    PLUGIN_UPLOAD_TOKEN_KEY,
    UPLOAD_API_PATH,
    UPLOAD_API_PORT,
    cloud_upload_root_for_user,
    db_connect,
    init_db,
    records_from_exported_jd_file,
)


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", (value or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned[:80] or "plugin_upload"


def lookup_user_id_by_token(token: str) -> int | None:
    init_db()
    with db_connect() as conn:
        row = conn.execute(
            "SELECT user_id FROM app_settings WHERE key = ? AND value = ?",
            (PLUGIN_UPLOAD_TOKEN_KEY, token.strip()),
        ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def save_payload_for_user(user_id: int, payload: dict[str, Any], prefix: str) -> Path:
    now = datetime.now()
    date_dir = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    title = str(payload.get("title") or payload.get("url") or prefix or "plugin_upload")
    target_dir = cloud_upload_root_for_user(user_id) / date_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{stamp}_{sanitize_filename(prefix)}_{sanitize_filename(title)}.json"
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path


class PluginUploadHandler(BaseHTTPRequestHandler):
    server_version = "CareerPilotUploadAPI/0.1"

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-CareerPilot-Upload-Token")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json(204, {})

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != UPLOAD_API_PATH:
            self._send_json(404, {"ok": False, "error": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length)
            body = json.loads(raw.decode("utf-8"))
            token = (self.headers.get("X-CareerPilot-Upload-Token") or body.get("token") or "").strip()
            payload = body.get("payload") if isinstance(body, dict) and isinstance(body.get("payload"), dict) else body
            prefix = sanitize_filename(str(body.get("prefix") or payload.get("type") or "upload")) if isinstance(body, dict) else "upload"
            if not token:
                self._send_json(401, {"ok": False, "error": "Missing upload token"})
                return
            if not isinstance(payload, dict):
                self._send_json(400, {"ok": False, "error": "Payload must be a JSON object"})
                return
            user_id = lookup_user_id_by_token(token)
            if not user_id:
                self._send_json(403, {"ok": False, "error": "Invalid upload token"})
                return
            saved_path = save_payload_for_user(user_id, payload, prefix)
            records = records_from_exported_jd_file(saved_path)
            self._send_json(
                200,
                {
                    "ok": True,
                    "user_id": user_id,
                    "saved_path": str(saved_path),
                    "job_count": len(records),
                },
            )
        except Exception as exc:  # pragma: no cover - defensive API boundary
            self._send_json(500, {"ok": False, "error": str(exc)})

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def run_upload_api_server(host: str = "0.0.0.0", port: int = UPLOAD_API_PORT) -> None:
    init_db()
    server = ThreadingHTTPServer((host, int(port)), PluginUploadHandler)
    print(f"CareerPilot upload API listening on http://{host}:{port}{UPLOAD_API_PATH}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run_upload_api_server(
        host=os.getenv("UPLOAD_API_HOST", "0.0.0.0"),
        port=int(os.getenv("UPLOAD_API_PORT", str(UPLOAD_API_PORT)) or str(UPLOAD_API_PORT)),
    )
