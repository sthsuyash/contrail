#!/usr/bin/env python3
"""Capture a dashboard screenshot for the README.

Plain `chrome --headless --screenshot` does not work here. The dashboard fetches
its data client-side, and `--virtual-time-budget` (the only wait knob that flag
offers) advances virtual time in a way that aborts pending real network
requests, so the page screenshots showing its "could not reach the API" state.

This waits on the rendered result instead: the tiles must show real numbers and
the flight table must have rows before anything is captured.

Usage:
    python3 scripts/screenshot.py [url] [output.png]
"""

from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

DEFAULT_URL = "http://localhost:3000"
DEFAULT_OUT = Path("docs/dashboard.png")

# The map is WebGL. Headless Chromium falls back to software rendering, which is
# slow but correct; without these flags the canvas can come out blank.
BROWSER_ARGS = [
    "--enable-unsafe-swiftshader",
    "--use-gl=angle",
    "--use-angle=swiftshader",
]


def capture(url: str, out: Path) -> int:
    out.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(args=BROWSER_ARGS)
        page = browser.new_page(
            viewport={"width": 1600, "height": 1080},
            # Retina-density output, so the image stays sharp in a README.
            device_scale_factor=2,
        )

        failures: list[str] = []
        page.on("pageerror", lambda exc: failures.append(str(exc)))
        page.on(
            "response",
            lambda r: failures.append(f"{r.status} {r.url}") if r.status >= 400 else None,
        )

        page.goto(url, wait_until="networkidle", timeout=30_000)

        # The tiles start as em-dashes and the table starts empty. Waiting for
        # both to fill is what distinguishes a loaded dashboard from a shell
        # that rendered before its data arrived.
        page.wait_for_function(
            """() => {
                const tiles = [...document.querySelectorAll('.tile .value')];
                const filled = tiles.filter(t => t.textContent.trim() !== '—');
                const rows = document.querySelectorAll('tbody tr').length;
                const noData = document.body.textContent.includes('No flights yet');
                return filled.length >= 4 && rows > 3 && !noData;
            }""",
            timeout=30_000,
        )

        # Let the WebGL layer finish painting the aircraft.
        page.wait_for_timeout(3000)

        page.screenshot(path=str(out), full_page=False)
        browser.close()

    if failures:
        print("warnings during capture:", file=sys.stderr)
        for failure in failures[:5]:
            print(f"  {failure}", file=sys.stderr)

    size_kb = out.stat().st_size // 1024
    print(f"wrote {out} ({size_kb} KB)")
    return 0


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUT
    return capture(url, out)


if __name__ == "__main__":
    raise SystemExit(main())
