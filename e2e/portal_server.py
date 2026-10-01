"""Start the portal (portal/serve.py) on a free port for tests and screenshot capture."""
import contextlib
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def reachable(url: str, timeout: float = 2) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout):
            return True
    except OSError:
        return False


@contextlib.contextmanager
def portal(collector: bool = False) -> Iterator[str]:
    """Yield the base URL of a portal server that stops when the block ends."""
    port = free_port()
    command = [sys.executable, str(ROOT / "portal" / "serve.py"), "--port", str(port)]
    if not collector:
        command.append("--no-collector")
    process = subprocess.Popen(command, cwd=ROOT / "portal", stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 15
        while not reachable(f"{url}/walkthrough.json", timeout=0.5):
            if process.poll() is not None:
                raise RuntimeError(f"portal exited: {process.stderr.read().decode(errors='replace')}")
            if time.time() > deadline:
                raise RuntimeError("portal didn't start within 15 s")
            time.sleep(0.2)
        yield url
    finally:
        process.terminate()
        process.wait(timeout=10)
