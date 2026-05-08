import html
import json
import os
import re
import threading
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
            script = browser_capture_loader_script_v3(
                endpoint=endpoint,
                upload_token=token,
                capture_core_path=capture_core_path,
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
                saved_path = save_capture_payload_for_user(user_id, payload, prefix)
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
def _capture_engine_source(capture_core_path: str) -> str:
    return Path(capture_core_path).read_text(encoding="utf-8")


def browser_capture_bookmarklet_code(
    upload_url: str,
    upload_token: str,
    *,
    capture_core_path: Path,
) -> str:
    script = f"""
(() => {{
  const endpoint = __UPLOAD_URL__;
  const token = __UPLOAD_TOKEN__;
  const extractorSource = __EXTRACTOR_SOURCE__;

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
    eval(extractorSource);
    if (!window.CareerPilotExtractor || typeof window.CareerPilotExtractor.collectAutoPayload !== "function") {{
      throw new Error("CareerPilot extractor failed to load.");
    }}
    const payload = await window.CareerPilotExtractor.collectAutoPayload({{
      fastMode: true,
      maxPages: 2,
      scrollSteps: 3,
      maxScrollRounds: 4,
      detailLimit: 0,
    }});
    payload.source = "careerpilot_bookmarklet";
    const data = await persistPayload(payload);
    const count = Number(data.job_count || payload.jobCount || (payload.text ? 1 : 0));
    alert(`CareerPilot 已接收${{count ? "：" + count + " 条内容" : "当前页面内容"}}。返回 CareerPilot 后直接点“同步采集结果”或去批量分析即可。`);
  }})().catch((error) => {{
    alert(`CareerPilot 采集失败：${{error.message}}\\n\\n请确认你已经登录招聘网站，并且 CareerPilot 上传地址可以从当前浏览器访问。`);
  }});
}})();
"""
    return (
        "javascript:"
        + script.strip()
        .replace("__UPLOAD_URL__", json.dumps(upload_url))
        .replace("__UPLOAD_TOKEN__", json.dumps(upload_token))
        .replace("__EXTRACTOR_SOURCE__", json.dumps(_capture_engine_source(str(capture_core_path))))
        .replace("\n", "")
    )


def browser_capture_loader_script_v2(
    *,
    endpoint: str,
    upload_token: str,
    capture_core_path: Path,
) -> str:
    return f"""
(() => {{
  const endpoint = {json.dumps(endpoint)};
  const token = {json.dumps(upload_token)};
  const extractorSource = {json.dumps(_capture_engine_source(str(capture_core_path)))};

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
    eval(extractorSource);
    if (!window.CareerPilotExtractor || typeof window.CareerPilotExtractor.collectAutoPayload !== "function") {{
      throw new Error("CareerPilot extractor failed to load.");
    }}
    const payload = await window.CareerPilotExtractor.collectAutoPayload({{
      fastMode: true,
      maxPages: 2,
      scrollSteps: 3,
      maxScrollRounds: 4,
      detailLimit: 0,
    }});
    payload.source = "careerpilot_bookmarklet";
    const data = await persistPayload(payload);
    const count = Number(data.job_count || payload.jobCount || (payload.text ? 1 : 0));
    alert(`CareerPilot 已接收${{count ? "：" + count + " 条内容" : "当前页面内容"}}。返回 CareerPilot 后点击“同步采集结果”即可。`);
  }})().catch((error) => {{
    alert(`CareerPilot 采集失败：${{error.message}}\\n\\n请确认你已经登录招聘网站，并且当前页面能访问 CareerPilot 上传地址。`);
  }});
}})();
""".strip()


def browser_capture_loader_script_v3(
    *,
    endpoint: str,
    upload_token: str,
    capture_core_path: Path,
) -> str:
    return f"""
(() => {{
  window.__careerpilotCaptureLoaderStarted = Date.now();
  const endpoint = {json.dumps(endpoint)};
  const token = {json.dumps(upload_token)};
  const extractorSource = {json.dumps(_capture_engine_source(str(capture_core_path)))};
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
      root.style.cssText = "position:fixed;right:16px;bottom:16px;z-index:2147483647;background:#111827;color:#fff;padding:12px 14px;border-radius:12px;box-shadow:0 12px 30px rgba(0,0,0,.28);font:13px/1.45 -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;min-width:220px";
      root.innerHTML = '<div id="' + overlayId + '-text">CareerPilot 正在采集中...</div><button id="' + stopButtonId + '" style="margin-top:8px;border:0;border-radius:8px;padding:6px 10px;background:#ef4444;color:#fff;cursor:pointer;">停止采集</button>';
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
    setOverlayText("CareerPilot 正在抓取岗位列表，请稍候...");
    const payload = await window.CareerPilotExtractor.collectAutoPayload({{
      fastMode: true,
      maxPages: 2,
      maxScrollRounds: 4,
      detailLimit: 0,
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
    alert(`CareerPilot 采集失败：${{error.message}}\\n\\n请确认你已经登录招聘网站，并且当前页面能访问 CareerPilot 上传地址。`);
  }}).finally(() => {{
    delete window[activeKey];
  }});
}})();
""".strip()


def browser_capture_bookmarklet_code_v2(
    upload_url: str,
    upload_token: str,
) -> str:
    parsed = urlparse(upload_url)
    script_path = capture_bookmarklet_script_path(parsed.path or "/api/capture-upload")
    loader_url = f"{parsed.scheme}://{parsed.netloc}{script_path}"
    script_src = f"{loader_url}?token={quote(upload_token)}&endpoint={quote(upload_url, safe=':/?&=%')}&t="
    script = f"""
(() => {{
  const id = "careerpilot-capture-loader";
  const endpoint = {json.dumps(upload_url)};
  const token = {json.dumps(upload_token)};
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
        jobCount: jobs.length,
        detailCount: 0,
        jobs: jobs.slice(0, 300),
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


def render_browser_capture_helper_v2(
    upload_url: str,
    upload_token: str,
    *,
    capture_core_path: Path,
    key_prefix: str,
    container: Any,
) -> None:
    bookmarklet = browser_capture_bookmarklet_code_v2(upload_url, upload_token)
    container.markdown(
        """
        <div class="cp-capture-panel">
            <div class="cp-capture-header">
                <div>
                    <div class="cp-capture-kicker">Capture Flow</div>
                    <div class="cp-capture-title">一键网页采集</div>
                </div>
                <div class="cp-capture-help-badge" tabindex="0">
                    ?
                    <div class="cp-capture-tooltip">
                        <strong>使用说明</strong><br/>
                        1. 把“一键采集”拖到浏览器书签栏。<br/>
                        2. 在已登录的招聘页面点击这个书签。<br/>
                        3. 回到 CareerPilot，点击“同步采集结果”查看导入内容。
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    container.markdown(
        (
            '<div class="cp-capture-actions">'
            '<a class="cp-capture-link" href="{href}">一键采集</a>'
            '<button type="button" class="cp-capture-ghost" '
            'onclick="navigator.clipboard.writeText(this.dataset.code);'
            "this.innerText='已复制书签代码';setTimeout(()=>this.innerText='复制书签代码',1500);\" "
            'data-code="{code}">复制书签代码</button>'
            "</div>"
        ).format(
            href=html.escape(bookmarklet, quote=True),
            code=html.escape(bookmarklet, quote=True),
        ),
        unsafe_allow_html=True,
    )
    with container.expander("备用方式：手动复制书签地址", expanded=False):
        container.text_area("采集书签地址", value=bookmarklet, height=120, key=f"{key_prefix}_bookmarklet_code")


def render_browser_capture_helper(
    upload_url: str,
    upload_token: str,
    *,
    capture_core_path: Path,
    key_prefix: str,
    container: Any,
) -> None:
    render_browser_capture_helper_v2(
        upload_url,
        upload_token,
        capture_core_path=capture_core_path,
        key_prefix=key_prefix,
        container=container,
    )
    return

    bookmarklet = browser_capture_bookmarklet_code_v2(upload_url, upload_token)
    container.markdown("##### 一键网页采集")
    container.caption("一个入口自动识别列表页或详情页，并把可用岗位内容同步回 CareerPilot。")
    parsed_upload_url = urlparse(upload_url)
    if parsed_upload_url.hostname in {"127.0.0.1", "localhost"}:
        container.warning("当前采集上传地址仍是本机地址。若要让其他电脑直接在浏览器使用，请在部署环境配置 APP_PUBLIC_URL 或 UPLOAD_API_PUBLIC_URL。")
    container.markdown(
        (
            '<a href="{href}" style="display:inline-block;padding:0.55rem 0.9rem;'
            'border-radius:999px;background:#0f766e;color:#fff;text-decoration:none;'
            'font-weight:700;">一键采集</a>'
        ).format(href=html.escape(bookmarklet, quote=True)),
        unsafe_allow_html=True,
    )
    container.caption("用法很简单：在招聘网站登录后点击这个书签。列表页会自动翻页并补抓详情，详情页会直接采当前岗位。")
    with container.expander("备用方式：复制采集书签代码", expanded=False):
        container.markdown(
            (
                '<button type="button" onclick="navigator.clipboard.writeText(this.dataset.code);'
                "this.innerText='已复制采集代码';setTimeout(()=>this.innerText='复制采集代码',1500);\" "
                'data-code="{code}" style="margin-top:0.2rem;padding:0.45rem 0.85rem;border-radius:10px;'
                'border:1px solid #c7d2fe;background:#eef2ff;cursor:pointer;">复制采集代码</button>'
            ).format(code=html.escape(bookmarklet, quote=True)),
            unsafe_allow_html=True,
        )
        container.text_area("采集书签地址", value=bookmarklet, height=120, key=f"{key_prefix}_bookmarklet_code")
