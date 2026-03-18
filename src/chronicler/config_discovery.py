"""
Resolve export_config.yml without requiring --config on every invocation.

Resolution order when --config is omitted:
1. CHRONICLER_CONFIG (path to YAML)
2. <installed chronicler package>/config/export_config.yml
3. ./export_config.yml (current working directory)
4. ./config/export_config.yml
5. ./chronicler/config/export_config.yml

If --config is given, that path is used regardless of the above.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def package_export_config_path() -> Path:
    """Path where export_config.yml is expected next to the installed package."""
    import chronicler

    return Path(chronicler.__file__).resolve().parent / "config" / "export_config.yml"


def resolve_export_config_path(
    cli_config: Optional[Path],
    cwd: Optional[Path] = None,
) -> Tuple[Optional[Path], str]:
    """
    Choose which config file to load.

    Returns:
        (path, provenance) — path is None only when no file should be read
        (caller treats as empty config). provenance is for logging.
    """
    cwd = cwd or Path.cwd()

    if cli_config is not None:
        resolved = cli_config.expanduser()
        return (resolved, f"explicit --config ({resolved})")

    env_raw = os.environ.get("CHRONICLER_CONFIG", "").strip()
    if env_raw:
        env_path = Path(env_raw).expanduser()
        if env_path.is_file():
            return (env_path.resolve(), "CHRONICLER_CONFIG")
        logger.warning(
            "CHRONICLER_CONFIG is set but file not found or not a file: %s; "
            "trying other locations",
            env_path,
        )

    pkg_path = package_export_config_path()
    if pkg_path.is_file():
        return (pkg_path, f"package default ({pkg_path})")

    candidates = [
        (cwd / "export_config.yml", "current directory: export_config.yml"),
        (cwd / "config" / "export_config.yml", "current directory: config/export_config.yml"),
        (
            cwd / "chronicler" / "config" / "export_config.yml",
            "current directory: chronicler/config/export_config.yml",
        ),
    ]
    for path, label in candidates:
        if path.is_file():
            return (path.resolve(), label)

    return (None, "none")
