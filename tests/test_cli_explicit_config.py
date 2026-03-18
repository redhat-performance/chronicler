"""CLI: explicit --config must be a regular file."""

import subprocess
import sys
from pathlib import Path


def test_cli_config_directory_exits_nonzero(tmp_path):
    empty = tmp_path / "in"
    empty.mkdir()
    bad = tmp_path / "notafile"
    bad.mkdir()
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "chronicler.run_postprocessing",
            "--input",
            str(empty),
            "--config",
            str(bad),
        ],
        cwd=str(tmp_path),
        env={**__import__("os").environ, "PYTHONPATH": str(src)},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1
    combined = proc.stderr + proc.stdout
    assert "does not exist or is not a file" in combined


def test_cli_config_missing_file_exits_nonzero(tmp_path):
    empty = tmp_path / "in"
    empty.mkdir()
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "chronicler.run_postprocessing",
            "--input",
            str(empty),
            "--config",
            str(tmp_path / "missing.yml"),
        ],
        cwd=str(tmp_path),
        env={**__import__("os").environ, "PYTHONPATH": str(src)},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1
    assert "does not exist or is not a file" in (proc.stderr + proc.stdout)
