from __future__ import annotations

import os
import subprocess
import sys
import time


def terminate_process(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()


def main() -> int:
    env = os.environ.copy()
    app_host = env.get("APP_HOST", "0.0.0.0")
    app_port = env.get("PORT") or env.get("APP_PORT") or "8503"
    upload_host = env.get("UPLOAD_API_HOST", "0.0.0.0")
    upload_port = env.get("UPLOAD_API_PORT", "8765")

    upload_proc = subprocess.Popen(
        [sys.executable, "careerpilot_upload_api.py"],
        env={**env, "UPLOAD_API_HOST": upload_host, "UPLOAD_API_PORT": upload_port},
    )
    streamlit_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "app.py",
            "--server.address",
            app_host,
            "--server.port",
            str(app_port),
            "--server.headless",
            "true",
        ],
        env=env,
    )

    try:
        while True:
            upload_code = upload_proc.poll()
            streamlit_code = streamlit_proc.poll()
            if upload_code is not None:
                terminate_process(streamlit_proc)
                return int(upload_code)
            if streamlit_code is not None:
                terminate_process(upload_proc)
                return int(streamlit_code)
            time.sleep(0.8)
    except KeyboardInterrupt:
        terminate_process(streamlit_proc)
        terminate_process(upload_proc)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
