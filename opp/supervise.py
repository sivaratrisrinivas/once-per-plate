"""Runs the watcher and N workers as child processes and restarts any that die.
Used by `make run` and by the demo container. A killed worker comes back as a new process."""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

N = int(os.environ.get("OPP_WORKERS", "3"))


def spawn(i: int | None):
    env = dict(os.environ)
    if i is None:
        return subprocess.Popen([sys.executable, "-m", "opp.watcher"], env=env)
    return subprocess.Popen([sys.executable, "-m", "opp.worker"], env=env)


def main():
    procs = {"watcher": spawn(None)}
    for i in range(N):
        procs[f"w{i}"] = spawn(i)

    def stop(*_):
        for p in procs.values():
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while True:
        time.sleep(0.5)
        for name, p in list(procs.items()):
            if p.poll() is not None:
                print(time.strftime("%H:%M:%S"), "supervisor", f"{name} exited ({p.returncode}), restarting",
                      flush=True)
                procs[name] = spawn(None if name == "watcher" else 0)


if __name__ == "__main__":
    main()
