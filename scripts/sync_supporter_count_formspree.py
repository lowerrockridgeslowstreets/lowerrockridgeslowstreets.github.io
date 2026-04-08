#!/usr/bin/env python3
"""
Fetch Formspree submission count and write supporter-count.json in the repo root.

Secrets (not in git): `.env` in repo root (gitignored), or FORMSPREE_ENV_FILE / environment variables.

Repo root: parent of scripts/ (this file lives in scripts/).

Requires: Python 3.9+ (stdlib only). Formspree Submissions API (paid plans).

Optional: GIT_PUSH=1 to git add/commit/push supporter-count.json (needs git + credentials).

macOS: repo under ~/Documents requires Full Disk Access for /usr/sbin/cron so cron can write + git push.
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = REPO_ROOT / ".env"
OUT_JSON = REPO_ROOT / "supporter-count.json"
API_BASE = "https://formspree.io/api/0/forms"

_BROWSER_HEADERS = (
    (
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ),
    ("Accept", "application/json, text/plain, */*"),
    ("Accept-Language", "en-US,en;q=0.9"),
)


def load_env_file(path: Path) -> None:
    try:
        if not path.is_file():
            return
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except OSError:
        return


def count_submissions(form_hash: str, api_key: str) -> int:
    total = 0
    offset = 0
    limit = 100
    while True:
        url = f"{API_BASE}/{form_hash}/submissions?limit={limit}&offset={offset}"
        req = urllib.request.Request(url)
        token = base64.b64encode(f":{api_key}".encode()).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
        for name, value in _BROWSER_HEADERS:
            req.add_header(name, value)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise SystemExit(f"Formspree HTTP {e.code}: {body[:500]}") from e
        except urllib.error.URLError as e:
            raise SystemExit(f"Formspree request failed: {e}") from e

        subs = data.get("submissions") or []
        n = len(subs)
        total += n
        if n < limit:
            break
        offset += limit
    return total


def main() -> int:
    env_path = os.environ.get("FORMSPREE_ENV_FILE")
    if env_path:
        load_env_file(Path(env_path))
    else:
        load_env_file(DEFAULT_ENV_PATH)

    api_key = (os.environ.get("FORMSPREE_API_KEY") or "").strip()
    form_hash = (os.environ.get("FORMSPREE_FORM_HASHID") or "xjgagzdj").strip()
    if not api_key:
        print(
            "Missing FORMSPREE_API_KEY. Add it to "
            f"{DEFAULT_ENV_PATH}, set FORMSPREE_ENV_FILE, or export in the environment.",
            file=sys.stderr,
        )
        return 1

    n = count_submissions(form_hash, api_key)
    payload = {"count": n}
    text = json.dumps(payload, indent=2) + "\n"

    if OUT_JSON.is_file():
        try:
            cur = json.loads(OUT_JSON.read_text(encoding="utf-8")).get("count")
        except (json.JSONDecodeError, OSError):
            cur = None
        if cur == n:
            print(f"No change: count remains {n}")
            return 0

    OUT_JSON.write_text(text, encoding="utf-8")
    print(f"Wrote {OUT_JSON} with count={n}")

    if os.environ.get("GIT_PUSH", "").strip() in ("1", "true", "yes"):
        try:
            subprocess.run(
                ["git", "add", "supporter-count.json"],
                cwd=REPO_ROOT,
                check=True,
            )
            subprocess.run(
                ["git", "commit", "-m", f"Sync supporter count from Formspree ({n})"],
                cwd=REPO_ROOT,
                check=True,
            )
            subprocess.run(["git", "push"], cwd=REPO_ROOT, check=True)
            print("Committed and pushed supporter-count.json")
        except subprocess.CalledProcessError as e:
            print(f"Git step failed (file was still updated on disk): {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
