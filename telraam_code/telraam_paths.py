"""Resolve the Telraam data directory (CSV + xlsx parent) without hard-coding machine paths.

Resolution order:
  1) Environment variable ``TELRAAM_DATA_DIR`` (absolute path to the ``telram_data`` folder).
  2) ``telraam_code/local_config.json`` (gitignored) with JSON object containing ``"data_dir"``.

Copy ``local_config.example.json`` to ``local_config.json`` and set ``data_dir`` to your
local ``telram_data`` path. Do not commit ``local_config.json``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

_CONFIG_FILENAME = "local_config.json"
_EXAMPLE_FILENAME = "local_config.example.json"


def get_telraam_data_dir() -> Path:
    env = os.environ.get("TELRAAM_DATA_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()

    base = Path(__file__).resolve().parent
    cfg = base / _CONFIG_FILENAME
    if cfg.is_file():
        try:
            data = json.loads(cfg.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise SystemExit(f"Invalid JSON in {cfg}: {e}") from e
        p = (data.get("data_dir") or data.get("telraam_data_dir") or "").strip()
        if p:
            return Path(p).expanduser().resolve()

    ex = base / _EXAMPLE_FILENAME
    hint = f"Copy {ex.name} to {_CONFIG_FILENAME}" if ex.is_file() else f"Create {_CONFIG_FILENAME}"
    raise SystemExit(
        "Telraam data directory is not configured.\n"
        "  Set environment variable TELRAAM_DATA_DIR to the absolute path of your "
        "telram_data folder, or\n"
        f"  {hint} with a JSON object: {{\"data_dir\": \"/absolute/path/to/telram_data\"}}\n"
        f"  (see {_EXAMPLE_FILENAME} in telraam_code/)."
    )
