"""Record the demo video with headless Playwright (captions drawn as a page overlay).
Run against a local server started with OPP_WORKERS=6 OPP_WORK_SECONDS=1.2.
Usage: python scripts/record_video.py http://127.0.0.1:8077 docs/chaos-output.txt
Prints the .webm path and the trim offset; scripts/make_video.sh converts it to MP4."""
import html
import json
import sys
import time
import urllib.request

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/")
CHAOS_TXT = open(sys.argv[2]).read().splitlines()
OUT = "video-work"

CAPTION_JS = """
(text) => {
  let el = document.getElementById('cap');
  if (!el) {
    el = document.createElement('div'); el.id = 'cap';
    el.style.cssText = 'position:fixed;left:50%;transform:translateX(-50%);bottom:20px;max-width:1140px;width:max-content;' +
      'background:rgba(15,23,42,.94);color:#fff;font:600 26px/1.35 system-ui,sans-serif;padding:13px 26px;' +
      'border-radius:12px;z-index:9999;text-align:center;box-shadow:0 6px 24px rgba(0,0,0,.25)';
    document.body.appendChild(el);
  }
  el.textContent = text; el.style.display = text ? 'block' : 'none';
}
"""
RING_JS = """
(sel) => {
  document.querySelectorAll('.ring').forEach(e => { e.classList.remove('ring'); e.style.outline=''; });
  if (!sel) return;
  const e = document.querySelector(sel); if (!e) return;
  e.classList.add('ring'); e.style.outline = '4px solid #f59e0b'; e.style.outlineOffset = '2px';
}
"""


def terminal_page():
    return """<html><head><style>
body{margin:0;background:#0b1220;height:720px;font:18px/1.45 ui-monospace,Menlo,Consolas,monospace;color:#e2e8f0}
.win{margin:14px 24px;border-radius:12px;background:#0f172a;box-shadow:0 10px 40px rgba(0,0,0,.5);overflow:hidden}
.top{background:#1e293b;padding:9px 14px;color:#94a3b8;font:13px system-ui}
.top i{display:inline-block;width:11px;height:11px;border-radius:50%;margin-right:6px;vertical-align:-1px}
pre{margin:0;padding:12px 18px;white-space:pre-wrap;font:inherit;min-height:470px}
.p{color:#4ade80;font-weight:700}.f{color:#f87171;font-weight:700}.d{color:#94a3b8}.cmd{color:#7dd3fc}.sum{color:#fde047;font-weight:700}
</style></head><body><div class="win"><div class="top"><i style="background:#f87171"></i><i style="background:#fbbf24"></i>
<i style="background:#4ade80"></i>&nbsp; chaos run on the build box: RabbitMQ 4.0.5, PostgreSQL 17, real worker processes</div>
<pre id="t"></pre></div></body></html>"""


def fmt(line):
    e = html.escape(line)
    if line.startswith("$"):
        return f'<span class="cmd">{e}</span>'
    if line.startswith("PASS"):
        name, rest = line[6:30], line[30:]
        rest = rest.rsplit("  [", 1)[0]
        return f'<span class="p">PASS</span>  {html.escape(name)} <span class="d">{html.escape(rest)}</span>'
    if line.startswith("FAIL"):
        return f'<span class="f">{e}</span>'
    if "passed in" in line:
        return f'<span class="sum">{e}</span>'
    return e


END_CARD = """<html><body style="margin:0;height:720px;display:flex;align-items:center;justify-content:center;
background:#0f172a;color:#fff;font-family:system-ui,sans-serif;text-align:center"><div>
<div style="font-size:56px;font-weight:750">Once Per Plate</div>
<div style="font-size:30px;margin-top:16px;color:#e2e8f0">One file, one run, one approval, one set of rows.</div>
<div style="font-size:22px;margin-top:26px;color:#94a3b8">FastAPI · RabbitMQ · Postgres · 16/16 chaos cases pass</div>
<div style="font-size:26px;margin-top:34px;color:#fde047">This is a pattern, not your code.</div>
<div style="font-size:17px;margin-top:26px;color:#64748b">Synthetic ELISpot data. Not affiliated with Scispot.
Built by Srinivas Sivaratri · github.com/sivaratrisrinivas/once-per-plate</div></div></body></html>"""


def post(path, body=None):
    req = urllib.request.Request(BASE + path, data=json.dumps(body or {}).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    return urllib.request.urlopen(req, timeout=60).read()


def state():
    return json.load(urllib.request.urlopen(BASE + "/api/state", timeout=30))


def main():
    post("/api/demo/reset")
    time.sleep(1)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1280, "height": 720}, record_video_dir=OUT,
                                  record_video_size={"width": 1280, "height": 720})
        page = ctx.new_page()
        t0 = time.time()
        page.goto(BASE + "/")
        page.wait_for_selector("#c-files")
        page.wait_for_timeout(900)
        cap = lambda s: page.evaluate(CAPTION_JS, s)  # noqa: E731
        ring = lambda s: page.evaluate(RING_JS, s)  # noqa: E731
        w = page.wait_for_timeout
        start = time.time() - t0

        # 0-6 s: hook
        cap("50 plate exports plus 10 duplicate copies land at once")
        w(300)
        page.click("text=Burst: 50 plates")
        w(2200)
        ring("#c-runs")
        cap("60 files in. 50 QC runs. Exactly one per plate.")
        w(3400)

        # 6-15 s: why
        ring("")
        cap("Scispot's Aug 26 blog lists a planned step: Smart Actions that run on their own when new files arrive.")
        w(4800)
        cap("Then a file that lands twice, or a worker that dies mid-run, must not become a duplicate or lost result.")
        w(4800)

        # 15-23 s: same file three times
        cap("The same export, dropped 3 times")
        page.click("text=Drop one plate 3 times")
        w(3000)
        ring(".why")
        cap("3 arrivals, 1 run. Each run is keyed on a hash of the file's content.")
        w(4200)

        # 23-40 s: kill a worker mid-run
        ring("")
        cap("Next plate. kill -9 its worker in the middle of the run.")
        page.click("text=Drop a plate and kill")
        page.wait_for_selector("#toast >> text=kill -9", timeout=20000)
        w(1600)
        ring("#evs")
        cap("RabbitMQ redelivers. The next worker waits out the dead worker's lease, then takes over.")
        page.wait_for_selector("#detail .b.PROPOSED", timeout=30000)
        w(1200)
        cap("Attempt 2, one result. The timeline shows who held the run and when.")
        w(4000)

        # 40-51 s: approval gate
        ring("#approve")
        cap("Nothing is written until a person approves.")
        w(3200)
        ring("")
        page.click("#approve")
        page.wait_for_selector("#detail .b.WRITTEN", timeout=15000)
        ring("#c-rows")
        cap("Approved: 96 Labsheet rows, written once.")
        w(3200)
        rid = next(r["id"] for r in state()["runs"] if r["state"] == "WRITTEN")
        for _ in range(5):
            post(f"/api/runs/{rid}/approve")
        cap("Approve sent 5 more times: no-op. Still 96 rows.")
        w(3600)
        ring("")

        # 51-63 s: chaos results
        cap("")
        page.set_content(terminal_page())
        cap("16 chaos cases against real RabbitMQ and Postgres")
        lines = [ln for ln in CHAOS_TXT if ln.strip()]
        for i, ln in enumerate(lines):
            page.evaluate("(h) => { document.getElementById('t').innerHTML += h + '\\n'; }", fmt(ln))
            w(380 if ln.startswith("PASS") else 600)
        cap("Double drops, half-written files, killed workers, broker and Postgres restarts: 16/16 PASS")
        w(5200)

        # end card
        cap("")
        page.set_content(END_CARD)
        w(6000)
        end = time.time() - t0
        path = page.video.path()
        ctx.close()
        browser.close()
    print(json.dumps({"video": path, "start": round(start, 2), "end": round(end, 2)}))


if __name__ == "__main__":
    main()
