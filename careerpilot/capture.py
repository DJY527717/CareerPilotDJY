import html
import json
import os
import re
import tempfile
import threading
import zipfile
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, quote, urlparse

_CAPTURE_API_SERVER: ThreadingHTTPServer | None = None
_CAPTURE_API_THREAD: threading.Thread | None = None
_CAPTURE_API_LOCK = threading.Lock()


def split_export_dir_env(raw: str) -> list[Path]:
    parts = re.split(r"[;\n\r]+", raw)
    return [Path(part.strip()).expanduser() for part in parts if part.strip()]


def resolve_jd_export_dirs(app_dir: Path) -> list[Path]:
    configured_dirs = split_export_dir_env(os.getenv("JD_EXPORT_DIRS", ""))
    single_configured_dir = os.getenv("JD_EXPORT_DIR", "").strip()
    if single_configured_dir:
        configured_dirs.extend(split_export_dir_env(single_configured_dir))

    default_dirs = [
        app_dir / "CareerPilot_JD",
        app_dir / "data" / "CareerPilot_JD",
    ]

    resolved: list[Path] = []
    seen: set[str] = set()
    for path in [*configured_dirs, *default_dirs]:
        normalized = str(path.resolve(strict=False))
        if normalized in seen:
            continue
        seen.add(normalized)
        resolved.append(path)
    return resolved


def sanitize_capture_filename(value: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", (value or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned[:80] or "capture_upload"


def resolve_capture_upload_bind_host() -> str:
    preferred = (
        os.getenv("UPLOAD_API_HOST", "").strip()
        or os.getenv("APP_HOST", "").strip()
        or "127.0.0.1"
    )
    return preferred or "127.0.0.1"


def capture_bookmarklet_script_path(upload_api_path: str) -> str:
    cleaned = (upload_api_path or "/api/capture-upload").strip() or "/api/capture-upload"
    if cleaned.endswith("/capture-upload"):
        return cleaned[: -len("/capture-upload")] + "/capture-bookmarklet.js"
    return cleaned.rstrip("/") + "/bookmarklet.js"


def bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def capture_options_from_job_limit(job_limit: int) -> dict[str, int | str]:
    max_jobs = bounded_int(job_limit, 30, 1, 200)
    return {
        "fastMode": "auto",
        "maxJobs": max_jobs,
        "maxPages": bounded_int((max_jobs + 14) // 15, 2, 1, 8),
        "maxScrollRounds": bounded_int((max_jobs + 4) // 5, 6, 4, 24),
        "detailLimit": max_jobs,
        "detailConcurrency": bounded_int((max_jobs + 9) // 10, 4, 2, 6),
    }


def is_capture_connection_test_payload(payload: dict[str, Any]) -> bool:
    return (
        payload.get("type") == "careerpilot_capture_connection_test"
        or payload.get("captureMode") == "connection_test"
        or payload.get("schemaVersion") == "careerpilot.capture.test"
    )


def ensure_embedded_capture_upload_service(
    *,
    capture_core_path: Path,
    upload_api_port: int,
    upload_api_path: str,
    lookup_user_id_by_capture_token: Callable[[str], int | None],
    save_capture_payload_for_user: Callable[[int, dict[str, Any], str], Path],
    records_from_exported_jd_file: Callable[[Path], list[dict[str, Any]]],
) -> None:
    global _CAPTURE_API_SERVER, _CAPTURE_API_THREAD

    class CareerPilotCaptureUploadHandler(BaseHTTPRequestHandler):
        server_version = "CareerPilotEmbeddedCaptureAPI/1.0"

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-CareerPilot-Upload-Token")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.send_header("Vary", "Origin, Access-Control-Request-Method, Access-Control-Request-Headers")
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self) -> None:  # noqa: N802
            self._send_json(204, {})

        def _send_javascript(self, status: int, script: str) -> None:
            body = script.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Vary", "Origin, Access-Control-Request-Method, Access-Control-Request-Headers")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != capture_bookmarklet_script_path(upload_api_path):
                self.send_response(404)
                self.end_headers()
                return

            params = parse_qs(parsed.query or "")
            token = str((params.get("token") or [""])[0]).strip()
            endpoint = str((params.get("endpoint") or [upload_api_path])[0]).strip() or upload_api_path
            max_jobs = bounded_int((params.get("maxJobs") or ["30"])[0], 30, 1, 200)
            capture_options = capture_options_from_job_limit(max_jobs)
            script = browser_capture_loader_script(
                endpoint=endpoint,
                upload_token=token,
                capture_core_path=capture_core_path,
                capture_options=capture_options,
            )
            self._send_javascript(200, script)

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != upload_api_path:
                self._send_json(404, {"ok": False, "error": "Not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length)
                body = json.loads(raw.decode("utf-8"))
                token = (self.headers.get("X-CareerPilot-Upload-Token") or body.get("token") or "").strip()
                payload = body.get("payload") if isinstance(body, dict) and isinstance(body.get("payload"), dict) else body
                prefix = (
                    sanitize_capture_filename(str(body.get("prefix") or payload.get("type") or "upload"))
                    if isinstance(body, dict)
                    else "upload"
                )
                if not token:
                    self._send_json(401, {"ok": False, "error": "Missing upload token"})
                    return
                if not isinstance(payload, dict):
                    self._send_json(400, {"ok": False, "error": "Payload must be a JSON object"})
                    return
                user_id = lookup_user_id_by_capture_token(token)
                if not user_id:
                    self._send_json(403, {"ok": False, "error": "Invalid upload token"})
                    return
                if is_capture_connection_test_payload(payload):
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "user_id": user_id,
                            "job_count": 0,
                            "parsedRecordCount": 0,
                            "schemaVersion": str(payload.get("schemaVersion") or ""),
                            "warning": "",
                            "test": True,
                        },
                    )
                    return
                saved_path = save_capture_payload_for_user(user_id, payload, prefix)
                records = records_from_exported_jd_file(saved_path)
                warning = "" if records else "Payload saved, but no job records were parsed."
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "user_id": user_id,
                        "saved_path": str(saved_path),
                        "savedPath": str(saved_path),
                        "job_count": len(records),
                        "parsedRecordCount": len(records),
                        "schemaVersion": str(payload.get("schemaVersion") or ""),
                        "warning": warning,
                    },
                )
            except Exception as exc:  # pragma: no cover - API boundary
                self._send_json(500, {"ok": False, "error": str(exc)})

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    with _CAPTURE_API_LOCK:
        if _CAPTURE_API_THREAD and _CAPTURE_API_THREAD.is_alive():
            return

        bind_host = resolve_capture_upload_bind_host()
        try:
            server = ThreadingHTTPServer((bind_host, int(upload_api_port)), CareerPilotCaptureUploadHandler)
        except OSError:
            return

        server.daemon_threads = True
        thread = threading.Thread(
            target=server.serve_forever,
            name="careerpilot-capture-upload",
            daemon=True,
        )
        thread.start()
        _CAPTURE_API_SERVER = server
        _CAPTURE_API_THREAD = thread


@lru_cache(maxsize=8)
def _capture_engine_source(capture_core_path: str, modified_ns: int) -> str:
    return Path(capture_core_path).read_text(encoding="utf-8")


def browser_capture_loader_script(
    *,
    endpoint: str,
    upload_token: str,
    capture_core_path: Path,
    capture_options: dict[str, Any] | None = None,
) -> str:
    options = capture_options_from_job_limit(
        int((capture_options or {}).get("maxJobs") or 30)
    )
    options.update({key: value for key, value in (capture_options or {}).items() if key in options})
    return f"""
(() => {{
  window.__careerpilotCaptureLoaderStarted = Date.now();
  const endpoint = {json.dumps(endpoint)};
  const token = {json.dumps(upload_token)};
  const extractorSource = {json.dumps(_capture_engine_source(str(capture_core_path), capture_core_path.stat().st_mtime_ns))};
  const captureOptions = {json.dumps(options, ensure_ascii=False)};
  const overlayId = "careerpilot-capture-status";
  const stopButtonId = "careerpilot-capture-stop";
  const activeKey = "__careerpilotCaptureRun";

  if (window[activeKey] && typeof window[activeKey].stop === "function") {{
    window[activeKey].stop();
  }}

  function ensureOverlay() {{
    let root = document.getElementById(overlayId);
    if (!root) {{
      root = document.createElement("div");
      root.id = overlayId;
      root.style.cssText = "position:fixed;right:16px;bottom:16px;z-index:2147483647;background:#111827;color:#fff;padding:12px 14px;border-radius:12px;box-shadow:0 12px 30px rgba(0,0,0,.28);font:13px/1.45 -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;min-width:240px;max-width:360px";
      root.innerHTML = '<div id="' + overlayId + '-text">CareerPilot 正在准备采集...</div><button id="' + stopButtonId + '" style="margin-top:8px;border:0;border-radius:8px;padding:6px 10px;background:#ef4444;color:#fff;cursor:pointer;">停止采集</button>';
      (document.body || document.documentElement).appendChild(root);
    }}
    return root;
  }}

  function setOverlayText(text) {{
    const root = ensureOverlay();
    const label = root.querySelector("#" + overlayId + "-text");
    if (label) label.textContent = text;
  }}

  function removeOverlay(delayMs) {{
    const root = document.getElementById(overlayId);
    if (!root) return;
    const remove = () => root.remove();
    if (delayMs > 0) setTimeout(remove, delayMs);
    else remove();
  }}

  async function persistPayload(payload) {{
    const response = await fetch(endpoint, {{
      method: "POST",
      mode: "cors",
      credentials: "omit",
      headers: {{
        "Content-Type": "application/json",
        "X-CareerPilot-Upload-Token": token,
      }},
      body: JSON.stringify({{ prefix: "bookmarklet", payload }}),
    }});
    const data = await response.json().catch(() => ({{}}));
    if (!response.ok || data.ok === false) {{
      throw new Error(data.error || `Upload failed (${{response.status}})`);
    }}
    return data;
  }}

  (async () => {{
    const controller = {{ cancelled: false, aborted() {{ return this.cancelled; }}, stop() {{ this.cancelled = true; }} }};
    window[activeKey] = controller;
    ensureOverlay();
    const stopButton = document.getElementById(stopButtonId);
    if (stopButton) stopButton.onclick = () => {{ controller.stop(); setOverlayText("CareerPilot 已停止当前采集。"); removeOverlay(1500); }};
    eval(extractorSource);
    if (!window.CareerPilotExtractor || typeof window.CareerPilotExtractor.collectAutoPayload !== "function") {{
      throw new Error("CareerPilot extractor failed to load.");
    }}
    setOverlayText(`CareerPilot 正在识别页面类型并深度采集，最多 ${{captureOptions.maxJobs || 30}} 条...`);
    const payload = await window.CareerPilotExtractor.collectAutoPayload({{
      ...captureOptions,
      signal: controller,
    }});
    if (controller.cancelled) {{
      throw new DOMException("Capture stopped by user.", "AbortError");
    }}
    payload.source = "careerpilot_bookmarklet";
    setOverlayText("CareerPilot 正在上传采集结果...");
    const data = await persistPayload(payload);
    const count = Number(data.job_count || payload.jobCount || (payload.text ? 1 : 0));
    setOverlayText(`CareerPilot 已采集 ${{count || 0}} 条，正在收尾...`);
    removeOverlay(1200);
    alert(`CareerPilot 已接收${{count ? "：" + count + " 条内容" : "当前页面内容"}}。返回 CareerPilot 后点击“同步采集结果”即可。`);
  }})().catch((error) => {{
    removeOverlay(0);
    if (error && (error.name === "AbortError" || /stopped by user/i.test(String(error.message || "")))) {{
      return;
    }}
    alert(`CareerPilot 采集失败：${{error.message}}\\n\\n请确认你已经登录招聘网站，并且当前页面可以连接 CareerPilot。`);
  }}).finally(() => {{
    delete window[activeKey];
  }});
}})();
""".strip()


def browser_capture_bookmarklet_code(
    upload_url: str,
    upload_token: str,
    *,
    capture_limit: int = 30,
) -> str:
    parsed = urlparse(upload_url)
    script_path = capture_bookmarklet_script_path(parsed.path or "/api/capture-upload")
    loader_url = f"{parsed.scheme}://{parsed.netloc}{script_path}"
    options = capture_options_from_job_limit(capture_limit)
    option_query = "".join(
        f"&{key}={quote(str(value))}"
        for key, value in options.items()
        if key != "fastMode"
    )
    script_src = f"{loader_url}?token={quote(upload_token)}&endpoint={quote(upload_url, safe=':/')}{option_query}&t="
    script = f"""
(() => {{
  const id = "careerpilot-capture-loader";
  const endpoint = {json.dumps(upload_url)};
  const token = {json.dumps(upload_token)};
  const captureLimit = {int(options["maxJobs"])};
  const loaderStartedKey = "__careerpilotCaptureLoaderStarted";
  const fallbackStartedKey = "__careerpilotCaptureFallbackStarted";
  const old = document.getElementById(id);
  if (old) old.remove();

  function cleanText(value) {{
    return String(value || "").replace(/[\\u00a0\\t]+/g, " ").replace(/\\n{{3,}}/g, "\\n\\n").trim();
  }}

  function nodeText(node) {{
    return cleanText(
      (node && (node.innerText || node.textContent))
      || (node && node.getAttribute && (node.getAttribute("aria-label") || node.getAttribute("title")))
      || ""
    );
  }}

  function salaryFrom(text) {{
    const match = cleanText(text).match(/\\d+(?:\\.\\d+)?\\s*[-~—至到]\\s*\\d+(?:\\.\\d+)?\\s*(?:[kK]|千|万|元\\s*\\/?\\s*天)(?:\\s*[·*xX]\\s*\\d+\\s*薪)?|\\d+(?:\\.\\d+)?\\s*[kK]\\s*(?:以上|\\+)?|面议/);
    return match ? cleanText(match[0]) : "";
  }}

  function absoluteUrl(href) {{
    try {{ return href ? new URL(href, location.href).href : ""; }} catch (_error) {{ return ""; }}
  }}

  function likelyJobUrl(href) {{
    const value = absoluteUrl(href).toLowerCase();
    return /job_detail|\\/job\\/|\\/jobs\\/|\\/intern\\/|\\/position\\/|jobid|job_id|positionid|postid|recruitid/.test(value);
  }}

  function fallbackPayload() {{
    const selectors = [
      "[class*='job']", "[class*='Job']", "[class*='position']", "[class*='Position']",
      "[class*='card']", "[class*='Card']", "[data-jobid]", "[data-position-id]",
      "[role='listitem']", "article", "li"
    ].join(",");
    const jobs = [];
    const seen = new Set();

    function add(node, forcedUrl) {{
      const text = nodeText(node);
      if (text.length < 20 || text.length > 5000) return;
      const salary = salaryFrom(text);
      const link = forcedUrl || absoluteUrl((node.querySelector && node.querySelector("a[href]") || node.closest && node.closest("a[href]") || {{}}).href || "");
      if (!salary && !likelyJobUrl(link) && text.length < 60) return;
      const lines = text.split(/\\n+/).map(cleanText).filter(Boolean);
      const title = lines.find((line) => line.length <= 90 && !salaryFrom(line)) || document.title || "采集岗位";
      const key = (link || "") + "|" + title + "|" + text.slice(0, 160);
      if (seen.has(key)) return;
      seen.add(key);
      jobs.push({{ title, salary, url: link, text }});
    }}

    Array.from(document.querySelectorAll(selectors)).slice(0, 600).forEach((node) => add(node, ""));
    Array.from(document.querySelectorAll("a[href]")).slice(0, 600).forEach((anchor) => {{
      if (!likelyJobUrl(anchor.href)) return;
      add(anchor.closest("article, li, section, div") || anchor, absoluteUrl(anchor.href));
    }});

    const pageText = cleanText((document.body && document.body.innerText) || document.documentElement.textContent || "");
    if (jobs.length) {{
      return {{
        type: "bookmarklet_fallback_list",
        title: document.title || "CareerPilot 采集",
        url: location.href,
        savedAt: new Date().toISOString(),
        jobCount: Math.min(jobs.length, captureLimit),
        detailCount: 0,
        jobs: jobs.slice(0, captureLimit),
        text: pageText.slice(0, 120000),
        source: "careerpilot_bookmarklet_fallback",
      }};
    }}
    return {{
      type: "bookmarklet_fallback_detail",
      title: document.title || "CareerPilot 采集",
      url: location.href,
      savedAt: new Date().toISOString(),
      text: pageText.slice(0, 120000),
      source: "careerpilot_bookmarklet_fallback",
    }};
  }}

  async function persistFallback(payload) {{
    const body = JSON.stringify({{ token, prefix: "bookmarklet_fallback", payload }});
    try {{
      const response = await fetch(endpoint, {{
        method: "POST",
        mode: "cors",
        credentials: "omit",
        headers: {{
          "Content-Type": "application/json",
          "X-CareerPilot-Upload-Token": token,
        }},
        body,
      }});
      const data = await response.json().catch(() => ({{}}));
      if (!response.ok || data.ok === false) throw new Error(data.error || `Upload failed (${{response.status}})`);
      return data;
    }} catch (error) {{
      if (navigator.sendBeacon) {{
        const ok = navigator.sendBeacon(endpoint, new Blob([body], {{ type: "application/json" }}));
        if (ok) return {{ ok: true, job_count: payload.jobCount || (payload.text ? 1 : 0) }};
      }}
      throw error;
    }}
  }}

  async function runFallback(reason) {{
    if (window[fallbackStartedKey] || window[loaderStartedKey]) return;
    window[fallbackStartedKey] = Date.now();
    const payload = fallbackPayload();
    if (!payload.text && !(payload.jobs && payload.jobs.length)) {{
      throw new Error("当前页面没有可采集文本。");
    }}
    const data = await persistFallback(payload);
    const count = Number(data.job_count || payload.jobCount || (payload.text ? 1 : 0));
    alert(`CareerPilot 已用备用采集接收${{count ? "：" + count + " 条内容" : "当前页面内容"}}。返回 CareerPilot 后点击“同步采集结果”即可。`);
  }}

  const s = document.createElement("script");
  s.id = id;
  s.async = true;
  s.onerror = () => runFallback("loader_error").catch((error) => alert(`CareerPilot 采集失败：${{error.message}}`));
  s.src = {json.dumps(script_src)} + Date.now();
  (document.head || document.documentElement || document.body).appendChild(s);
  setTimeout(() => {{
    if (!window[loaderStartedKey]) {{
      runFallback("loader_timeout").catch((error) => alert(`CareerPilot 采集失败：${{error.message}}`));
    }}
  }}, 2200);
}})();
"""
    return "javascript:" + script.strip().replace("\n", "")


def browser_capture_install_page(bookmarklet: str) -> str:
    escaped_href = html.escape(bookmarklet, quote=True)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CareerPilot 一键采集安装页</title>
  <style>
    body {{
      margin: 0;
      padding: 32px;
      background: #f7f4ec;
      color: #1f2933;
      font: 15px/1.65 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      max-width: 760px;
      margin: 0 auto;
      background: #fffdfa;
      border: 1px solid #e7decc;
      border-radius: 16px;
      padding: 28px;
      box-shadow: 0 18px 42px rgba(31, 41, 51, 0.08);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 24px;
    }}
    .bookmarklet {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      margin: 18px 0;
      min-height: 44px;
      padding: 0 18px;
      border-radius: 12px;
      border: 1px solid #65a69a;
      background: #eef8f5;
      color: #0f766e;
      font-weight: 800;
      text-decoration: none;
    }}
    .steps {{
      margin-top: 12px;
      color: #4b5563;
    }}
  </style>
</head>
<body>
  <main>
    <h1>CareerPilot 一键采集安装页</h1>
    <p>把下面这个“一键采集”拖到浏览器书签栏。之后在已登录的招聘网页点击书签，再回到 CareerPilot 同步采集结果。</p>
    <p><a class="bookmarklet" href="{escaped_href}">一键采集</a></p>
    <div class="steps">
      <div>1. 将“一键采集”拖到浏览器书签栏。</div>
      <div>2. 打开已登录的招聘岗位页或列表页。</div>
      <div>3. 点击书签后回到 CareerPilot 同步结果。</div>
    </div>
  </main>
</body>
</html>
"""


def test_capture_upload_connection(upload_url: str, upload_token: str) -> tuple[bool, str]:
    try:
        import requests
    except Exception as exc:
        return False, f"无法加载 requests：{exc}"
    payload = {
        "schemaVersion": "careerpilot.capture.test",
        "type": "careerpilot_capture_connection_test",
        "captureMode": "connection_test",
        "title": "CareerPilot 上传连接测试",
        "url": upload_url,
        "jobs": [],
    }
    try:
        response = requests.post(
            upload_url,
            headers={
                "Content-Type": "application/json",
                "X-CareerPilot-Upload-Token": upload_token,
            },
            json={"prefix": "connection_test", "payload": payload},
            timeout=8,
        )
    except requests.exceptions.Timeout:
        return False, "接口请求超时，请检查 upload_url 是否可访问。"
    except requests.exceptions.ConnectionError as exc:
        return False, f"接口不可访问或网络被拦截：{exc}"
    except requests.exceptions.RequestException as exc:
        return False, f"网络请求失败，可能是 CORS、代理或网络问题：{exc}"

    data: dict[str, Any] = {}
    try:
        data = response.json()
    except Exception:
        data = {}
    if response.status_code in {401, 403}:
        return False, data.get("error") or "token 错误或无权限。"
    if not response.ok:
        return False, data.get("error") or f"接口返回 HTTP {response.status_code}。"
    if data.get("ok") is False:
        return False, str(data.get("error") or "上传服务返回失败。")
    return True, "上传服务可用"


def browser_extension_manifest_path(app_dir: Path) -> Path:
    return app_dir / "browser_extension" / "manifest.json"


def browser_extension_is_packable(app_dir: Path) -> bool:
    manifest_path = browser_extension_manifest_path(app_dir)
    if not manifest_path.exists():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return False
    return bool(manifest.get("manifest_version") and manifest.get("name"))


def build_browser_extension_zip(app_dir: Path, capture_core_path: Path) -> bytes:
    extension_dir = app_dir / "browser_extension"
    if not browser_extension_is_packable(app_dir):
        raise ValueError("browser_extension 缺少完整 manifest.json，暂不能打包。")
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "careerpilot_browser_extension.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in extension_dir.rglob("*"):
                if not path.is_file():
                    continue
                relative = path.relative_to(extension_dir)
                if relative.as_posix() == "capture_core.js":
                    archive.writestr(str(relative).replace("\\", "/"), capture_core_path.read_text(encoding="utf-8"))
                else:
                    archive.write(path, str(relative).replace("\\", "/"))
        return zip_path.read_bytes()


def render_browser_capture_helper_ui(
    upload_url: str,
    upload_token: str,
    *,
    capture_core_path: Path,
    app_dir: Path | None = None,
    key_prefix: str,
    container: Any,
) -> None:
    capture_limit = bounded_int(os.getenv("CAREERPILOT_CAPTURE_LIMIT", "30"), 30, 1, 200)
    bookmarklet = browser_capture_bookmarklet_code(
        upload_url,
        upload_token,
        capture_limit=capture_limit,
    )
    container.caption("优先使用书签采集；复制书签代码是备用安装方式；浏览器扩展是可选方式；云端使用时请确保 upload_url 是公网地址。")
    container.markdown(
        """
        <div class="cp-capture-actions">
            <a class="cp-capture-link" href="{href}" aria-describedby="{help_id}">一键采集</a>
            <span class="cp-capture-help-badge" tabindex="0" role="button" aria-label="一键采集功能说明和用法说明">?</span>
            <span class="cp-capture-tooltip" id="{help_id}" role="tooltip">
                <strong>隐藏说明</strong><br/>
                功能：自动识别岗位详情页或列表页；详情页直接采当前岗位，列表页会滚动、翻页并尽量补全详情页。<br/>
                用法：把“一键采集”拖到浏览器书签栏；在已登录招聘网站打开岗位页或列表页后点击书签；回到 CareerPilot 点击“同步采集结果”。<br/>
                提示：默认最多采集 {limit} 条，可通过 CAREERPILOT_CAPTURE_LIMIT 调整。
            </span>
        </div>
        """.format(
            href=html.escape(bookmarklet, quote=True),
            help_id=html.escape(f"{key_prefix}_capture_help", quote=True),
            limit=capture_limit,
        ),
        unsafe_allow_html=True,
    )
    with container.expander("备用安装与连接测试", expanded=False):
        container.write("复制书签代码后，可以在浏览器里手动新建书签，并把下面内容粘贴到书签网址。")
        try:
            import streamlit.components.v1 as components

            components.html(
                f"""
                <button id="cp-copy-{html.escape(key_prefix, quote=True)}" style="border:1px solid #0f766e;border-radius:8px;background:#eef8f5;color:#0f766e;padding:8px 12px;font-weight:700;cursor:pointer;">复制书签代码</button>
                <span id="cp-copy-status-{html.escape(key_prefix, quote=True)}" style="margin-left:8px;color:#64748b;font:13px sans-serif;"></span>
                <script>
                (() => {{
                  const code = {json.dumps(bookmarklet)};
                  const button = document.getElementById("cp-copy-{html.escape(key_prefix, quote=True)}");
                  const status = document.getElementById("cp-copy-status-{html.escape(key_prefix, quote=True)}");
                  if (!button) return;
                  button.addEventListener("click", async () => {{
                    try {{
                      await navigator.clipboard.writeText(code);
                      status.textContent = "已复制";
                    }} catch (error) {{
                      status.textContent = "自动复制失败，请手动复制下方文本";
                    }}
                  }});
                }})();
                </script>
                """,
                height=44,
            )
        except Exception:
            pass
        container.text_area(
            "书签代码",
            value=bookmarklet,
            height=140,
            key=f"{key_prefix}_bookmarklet_code",
            label_visibility="collapsed",
        )
        if container.button("复制书签代码", key=f"{key_prefix}_copy_bookmarklet"):
            container.code(bookmarklet, language="javascript")
            container.info("已显示完整书签代码。若浏览器没有自动复制，请手动复制上方文本框内容。")
        if container.button("测试上传连接", key=f"{key_prefix}_test_upload"):
            ok, message = test_capture_upload_connection(upload_url, upload_token)
            if ok:
                container.success("上传服务可用")
            else:
                container.error(message)
                container.caption("测试失败不影响继续复制书签，也不影响导入本地导出的 JSON。")
        resolved_app_dir = app_dir or capture_core_path.resolve().parent
        if browser_extension_is_packable(resolved_app_dir):
            try:
                zip_bytes = build_browser_extension_zip(resolved_app_dir, capture_core_path)
                container.download_button(
                    "下载浏览器扩展包",
                    data=zip_bytes,
                    file_name="careerpilot_browser_extension.zip",
                    mime="application/zip",
                    key=f"{key_prefix}_download_extension",
                )
            except Exception as exc:
                container.warning(f"浏览器扩展包暂不可用：{exc}")


def render_browser_capture_helper(
    upload_url: str,
    upload_token: str,
    *,
    capture_core_path: Path,
    app_dir: Path | None = None,
    key_prefix: str,
    container: Any,
) -> None:
    render_browser_capture_helper_ui(
        upload_url,
        upload_token,
        capture_core_path=capture_core_path,
        app_dir=app_dir,
        key_prefix=key_prefix,
        container=container,
    )
