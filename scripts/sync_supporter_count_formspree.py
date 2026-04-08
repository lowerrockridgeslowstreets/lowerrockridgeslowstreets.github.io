#!/usr/bin/env python3
"""
Fetch Formspree submission count and write ../supporter-count.json if it changed.

Secrets: NOT in this repo. Use either:
  - Environment variables FORMSPREE_API_KEY (+ optional FORMSPREE_FORM_HASHID), or
  - A gitignored file: private/formspree-sync.env (copy from scripts/formspree-sync.env.example)

Requires: Python 3.9+ (stdlib only). Formspree Submissions API (paid plans).

Optional: GIT_PUSH=1 to git add/commit/push supporter-count.json from repo root (needs git + credentials).
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
DEFAULT_ENV_PATH = REPO_ROOT / "private" / "formspree-sync.env"
OUT_JSON = REPO_ROOT / "supporter-count.json"
API_BASE = "https://formspree.io/api/0/forms"


def load_env_file(path: Path) -> None:
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


def count_submissions(form_hash: str, api_key: str) -> int:
    total = 0
    offset = 0
    limit = 100
    while True:
        url = f"{API_BASE}/{form_hash}/submissions?limit={limit}&offset={offset}"
        req = urllib.request.Request(url)
        token = base64.b64encode(f":{api_key}".encode()).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
        req.add_header("Accept", "application/json")
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
        home_cfg = Path.home() / ".config" / "rockridge-formspree.env"
        load_env_file(home_cfg)

    api_key = (os.environ.get("FORMSPREE_API_KEY") or "").strip()
    form_hash = (os.environ.get("FORMSPREE_FORM_HASHID") or "xjgagzdj").strip()
    if not api_key:
        print(
            "Missing FORMSPREE_API_KEY. Set it in the environment or in "
            f"{DEFAULT_ENV_PATH} (copy from scripts/formspree-sync.env.example).",
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
