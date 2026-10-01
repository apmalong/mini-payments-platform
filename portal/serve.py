"""Serves the portal, the project's docs read-only, and service status history.

Only portal/ and Markdown/images under docs/ are reachable: serving the project root instead
would expose .env and the data files. Standard library only; any of the project's Pythons work.

    python portal/serve.py          # http://localhost:8099
    python portal/serve.py --port 8199 --no-collector   # e2e tests: no background checks
"""
import argparse
import http.server
import json
import urllib.parse
from pathlib import Path

import status

ROOT = Path(__file__).resolve().parent.parent
PORTAL = ROOT / "portal"
DOCS = ROOT / "docs"
DOC_TYPES = {".md": "text/markdown; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png"}
GROUPS = {"": "Reference", "runbooks": "Runbooks", "adr": "Decisions (ADRs)"}


def doc_index() -> list[dict]:
    docs = []
    for path in sorted(DOCS.rglob("*.md")):
        if "template" in path.stem:
            continue
        relative = path.relative_to(DOCS).as_posix()
        first_heading = next((line.lstrip("# ").strip() for line in path.read_text(encoding="utf-8").splitlines()
                              if line.startswith("# ")), path.stem)
        docs.append({"path": relative, "title": first_heading.replace("`", ""),
                     "group": GROUPS.get(path.parent.relative_to(DOCS).as_posix().replace(".", ""), "Reference")})
    # A folder's README (its index) comes first in its group.
    return sorted(docs, key=lambda d: (d["group"], not d["path"].endswith("README.md"), d["path"]))


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PORTAL), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")  # docs change while you work
        super().end_headers()

    def do_GET(self):
        path = urllib.parse.unquote(urllib.parse.urlsplit(self.path).path)
        if path == "/status/current":
            return self.send_bytes(json.dumps(status.current()).encode(), "application/json")
        if path == "/status/history":
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            body = json.dumps(status.history(query.get("range", ["24h"])[0])).encode()
            return self.send_bytes(body, "application/json")
        if path == "/docs/index.json":
            return self.send_bytes(json.dumps(doc_index()).encode(), "application/json")
        if path.startswith("/docs/"):
            target = (DOCS / path[len("/docs/"):]).resolve()
            if not target.is_relative_to(DOCS) or target.suffix not in DOC_TYPES or not target.is_file():
                return self.send_error(404)
            return self.send_bytes(target.read_bytes(), DOC_TYPES[target.suffix])
        return super().do_GET()

    def send_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serve the portal")
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--no-collector", action="store_true", help="don't check services in the background")
    args = parser.parse_args()
    if not args.no_collector:
        status.start_collector()  # checks every service every 30 s while the portal runs
    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    collector = "off" if args.no_collector else "running"
    print(f"portal on http://localhost:{args.port} (status collector {collector})", flush=True)
    server.serve_forever()
