

# scripts/capture_dashboard.py
from __future__ import annotations

import subprocess
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

REPORT_DIR = Path("monitoring/reports")
SCREENSHOT_PATH = REPORT_DIR / "streamlit_dashboard.png"


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Start Streamlit in background
    proc = subprocess.Popen(
        ["python", "-m", "streamlit", "run", "dashboard/app.py", "--server.headless=true", "--server.port=8501"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        # Give it time to start
        time.sleep(8)

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            page.goto("http://127.0.0.1:8501", wait_until="networkidle", timeout=120_000)
            time.sleep(2)
            page.screenshot(path=str(SCREENSHOT_PATH), full_page=True)
            browser.close()

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    print(f"Saved dashboard screenshot to {SCREENSHOT_PATH}")


if __name__ == "__main__":
    main()
