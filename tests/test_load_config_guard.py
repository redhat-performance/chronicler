"""Tests for load_config() non-file path handling."""

from pathlib import Path

from chronicler.run_postprocessing import load_config


def test_load_config_directory_returns_empty(tmp_path):
    d = tmp_path / "cfgdir"
    d.mkdir()
    assert load_config(d) == {}


def test_load_config_missing_path_returns_empty(tmp_path):
    assert load_config(tmp_path / "nonexistent.yml") == {}


def test_load_config_valid_yaml(tmp_path):
    f = tmp_path / "c.yml"
    f.write_text("opensearch:\n  url: http://x\n")
    cfg = load_config(f)
    assert cfg["opensearch"]["url"] == "http://x"
