"""Minimal HTTP API server for the CareerPilot React frontend slice.

Run with:
    .\.venv\Scripts\python.exe -m careerpilot_api.app
"""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse

from careerpilot_api import __version__
from careerpilot_api.batch_screener import screen_batch_jds_for_api
from careerpilot_api.auth_service import (
    get_auth_session_for_api,
    get_me_for_api,
    get_workspaces_for_api,
    login_for_api,
    logout_for_api,
    register_for_api,
    select_workspace_for_api,
)
from careerpilot_api.decision_service import evaluate_decision_for_api
from careerpilot_api.mock_service import (
    analyze_jd_for_api,
    get_bootstrap_data,
    get_ranked_jobs,
    get_settings_summary,
    match_resume_for_api,
    parse_resume_for_api,
)
from careerpilot_api.product_roles import get_product_roles
from careerpilot_api.report_service import summarize_interview_for_api
from careerpilot_api.role_boundaries import get_role_boundaries
from careerpilot_api.role_router import get_role_routes
from careerpilot_api.schemas import ApiEnvelope, to_jsonable
from careerpilot_api.settings_service import validate_settings_for_api


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
JsonDict = dict[str, Any]


class CareerPilotApiHandler(BaseHTTPRequestHandler):
    server_version = f"CareerPilotApi/{__version__}"

    def do_OPTIONS(self) -> None:
        self._send_empty(HTTPStatus.NO_CONTENT)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        routes: dict[str, Callable[[], Any]] = {
            "/api/health": self._health,
            "/api/bootstrap": get_bootstrap_data,
            "/api/auth/session": get_auth_session_for_api,
            "/api/me": get_me_for_api,
            "/api/workspaces": get_workspaces_for_api,
            "/api/settings/summary": get_settings_summary,
            "/api/jobs/ranked": get_ranked_jobs,
            "/api/product-roles": get_product_roles,
            "/api/role-boundaries": get_role_boundaries,
            "/api/role-routes": get_role_routes,
        }
        handler = routes.get(parsed.path)
        if handler is None:
            self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return
        self._send_json(handler())

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        payload = self._read_json_body()
        if payload is None:
            return

        routes: dict[str, Callable[[JsonDict], Any]] = {
            "/api/auth/register": register_for_api,
            "/api/auth/login": login_for_api,
            "/api/auth/logout": logout_for_api,
            "/api/workspaces/select": select_workspace_for_api,
            "/api/settings/validate": validate_settings_for_api,
            "/api/jd/analyze": analyze_jd_for_api,
            "/api/jd/batch-screen": screen_batch_jds_for_api,
            "/api/resume/parse": parse_resume_for_api,
            "/api/resume/match": match_resume_for_api,
            "/api/decision/evaluate": evaluate_decision_for_api,
            "/api/report/interview-summary": summarize_interview_for_api,
        }
        handler = routes.get(parsed.path)
        if handler is None:
            self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return
        self._send_json(handler(payload))

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _health(self) -> JsonDict:
        return {"status": "ok", "version": __version__}

    def _read_json_body(self) -> JsonDict | None:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        raw_body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_error(HTTPStatus.BAD_REQUEST, "Request body must be valid JSON")
            return None
        if not isinstance(payload, dict):
            self._send_error(HTTPStatus.BAD_REQUEST, "Request body must be a JSON object")
            return None
        return payload

    def _send_json(self, data: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        envelope = ApiEnvelope(ok=True, data=data)
        body = json.dumps(to_jsonable(envelope), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._send_common_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        body = json.dumps({"ok": False, "error": {"message": message}}, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._send_common_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_empty(self, status: HTTPStatus) -> None:
        self.send_response(status)
        self._send_common_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_common_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store")


def create_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), CareerPilotApiHandler)


def main() -> None:
    server = create_server()
    print(f"CareerPilot API mock server listening on http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping CareerPilot API mock server")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
