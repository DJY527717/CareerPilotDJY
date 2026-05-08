from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from tornado import httpclient, httputil, ioloop, web, websocket

from careerpilot.capture import capture_bookmarklet_script_path


PROJECT_DIR = Path(__file__).resolve().parent
APP_FILE = PROJECT_DIR / "app.py"
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except ValueError:
        return default


def normalize_path(value: str, default: str) -> str:
    cleaned = (value or default).strip() or default
    return cleaned if cleaned.startswith("/") else f"/{cleaned}"


def build_upstream_url(base: str, request_uri: str, scheme: str) -> str:
    parsed_base = urlsplit(base)
    parsed_request = urlsplit(request_uri)
    return urlunsplit(
        (
            scheme,
            parsed_base.netloc,
            parsed_request.path,
            parsed_request.query,
            "",
        )
    )


class ReverseProxyHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")

    def initialize(
        self,
        *,
        streamlit_base: str,
        upload_base: str,
        upload_api_path: str,
        bookmarklet_path: str,
    ) -> None:
        self.streamlit_base = streamlit_base
        self.upload_base = upload_base
        self.upload_api_path = upload_api_path
        self.bookmarklet_path = bookmarklet_path

    def target_base(self) -> str:
        path = urlsplit(self.request.uri).path
        if path in {self.upload_api_path, self.bookmarklet_path}:
            return self.upload_base
        return self.streamlit_base

    async def proxy(self) -> None:
        target = build_upstream_url(self.target_base(), self.request.uri, "http")
        headers = httputil.HTTPHeaders()
        for name, value in self.request.headers.get_all():
            if name.lower() not in HOP_BY_HOP_HEADERS and name.lower() != "host":
                headers.add(name, value)

        body = self.request.body if self.request.method not in {"GET", "HEAD"} else None
        client = httpclient.AsyncHTTPClient()
        try:
            response = await client.fetch(
                target,
                method=self.request.method,
                headers=headers,
                body=body,
                follow_redirects=False,
                raise_error=False,
                decompress_response=True,
            )
        except Exception as exc:
            self.set_status(502)
            self.write(f"CareerPilot upstream is not ready: {exc}")
            return

        self.set_status(response.code, response.reason)
        for name, value in response.headers.get_all():
            lowered = name.lower()
            if lowered in HOP_BY_HOP_HEADERS or lowered in {"content-length", "content-encoding"}:
                continue
            self.add_header(name, value)
        if self.request.method != "HEAD" and response.body:
            self.write(response.body)

    async def get(self) -> None:
        await self.proxy()

    async def head(self) -> None:
        await self.proxy()

    async def post(self) -> None:
        await self.proxy()

    async def put(self) -> None:
        await self.proxy()

    async def patch(self) -> None:
        await self.proxy()

    async def delete(self) -> None:
        await self.proxy()

    async def options(self) -> None:
        await self.proxy()


class StreamlitWebSocketProxy(websocket.WebSocketHandler):
    def initialize(self, *, streamlit_base: str) -> None:
        self.streamlit_base = streamlit_base

    def check_origin(self, origin: str) -> bool:
        return True

    async def open(self) -> None:
        target = build_upstream_url(self.streamlit_base, self.request.uri, "ws")
        headers = httputil.HTTPHeaders()
        for name, value in self.request.headers.get_all():
            if name.lower() not in HOP_BY_HOP_HEADERS and name.lower() not in {"host", "origin"}:
                headers.add(name, value)
        self.upstream = await websocket.websocket_connect(target, headers=headers)
        self.relay_task = asyncio.create_task(self.relay_from_upstream())

    async def relay_from_upstream(self) -> None:
        try:
            while True:
                message = await self.upstream.read_message()
                if message is None:
                    break
                self.write_message(message, binary=isinstance(message, bytes))
        finally:
            self.close()

    def on_message(self, message: str | bytes) -> None:
        if getattr(self, "upstream", None):
            self.upstream.write_message(message, binary=isinstance(message, bytes))

    def on_close(self) -> None:
        if getattr(self, "upstream", None):
            self.upstream.close()
        task = getattr(self, "relay_task", None)
        if task:
            task.cancel()


def start_streamlit(host: str, port: int, upload_host: str, upload_port: int) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env.setdefault("UPLOAD_API_HOST", upload_host)
    env.setdefault("UPLOAD_API_PORT", str(upload_port))
    env["CAPTURE_UPLOAD_SAME_ORIGIN"] = "1"
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(APP_FILE),
        "--server.address",
        host,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]
    return subprocess.Popen(command, cwd=str(PROJECT_DIR), env=env)


def main() -> None:
    load_dotenv(PROJECT_DIR / ".env")

    public_host = os.getenv("APP_HOST", "0.0.0.0").strip() or "0.0.0.0"
    public_port = env_int("APP_PORT", 8000)
    streamlit_host = os.getenv("STREAMLIT_INTERNAL_HOST", "127.0.0.1").strip() or "127.0.0.1"
    streamlit_port = env_int("STREAMLIT_INTERNAL_PORT", public_port + 1)
    upload_host = os.getenv("UPLOAD_API_HOST", "127.0.0.1").strip() or "127.0.0.1"
    upload_port = env_int("UPLOAD_API_PORT", 8765)
    upload_api_path = normalize_path(os.getenv("UPLOAD_API_PATH", "/api/capture-upload"), "/api/capture-upload")
    bookmarklet_path = capture_bookmarklet_script_path(upload_api_path)

    streamlit_process = start_streamlit(streamlit_host, streamlit_port, upload_host, upload_port)
    streamlit_base = f"http://{streamlit_host}:{streamlit_port}"
    upload_base = f"http://{upload_host}:{upload_port}"

    app = web.Application(
        [
            (r"/_stcore/stream", StreamlitWebSocketProxy, {"streamlit_base": streamlit_base}),
            (r"/stream", StreamlitWebSocketProxy, {"streamlit_base": streamlit_base}),
            (
                r"/.*",
                ReverseProxyHandler,
                {
                    "streamlit_base": streamlit_base,
                    "upload_base": upload_base,
                    "upload_api_path": upload_api_path,
                    "bookmarklet_path": bookmarklet_path,
                },
            ),
        ]
    )
    app.listen(public_port, address=public_host)
    loop = ioloop.IOLoop.current()

    def shutdown(*_args: object) -> None:
        if streamlit_process.poll() is None:
            streamlit_process.terminate()
        loop.add_callback(loop.stop)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    print(
        f"CareerPilot serving on {public_host}:{public_port}; "
        f"Streamlit upstream {streamlit_base}; capture API {upload_base}{upload_api_path}",
        flush=True,
    )
    try:
        loop.start()
    finally:
        if streamlit_process.poll() is None:
            streamlit_process.terminate()
            try:
                streamlit_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                streamlit_process.kill()


if __name__ == "__main__":
    main()
