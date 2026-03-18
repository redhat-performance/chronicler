"""Tests for export_config.yml resolution order."""

from pathlib import Path

import pytest

from chronicler import config_discovery


@pytest.fixture
def isolated_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_explicit_config_always_wins(isolated_cwd, monkeypatch):
    p = isolated_cwd / "my.yml"
    p.write_text("opensearch:\n  url: http://x\n")
    monkeypatch.delenv("CHRONICLER_CONFIG", raising=False)
    path, prov = config_discovery.resolve_export_config_path(p)
    assert path == p
    assert "explicit" in prov


def test_chronicler_config_env(isolated_cwd, monkeypatch):
    env_file = isolated_cwd / "from_env.yml"
    env_file.write_text("opensearch:\n  url: http://env\n")
    monkeypatch.setenv("CHRONICLER_CONFIG", str(env_file))
    path, prov = config_discovery.resolve_export_config_path(None)
    assert path.resolve() == env_file.resolve()
    assert "CHRONICLER_CONFIG" in prov


def test_chronicler_config_env_missing_falls_through(isolated_cwd, monkeypatch):
    monkeypatch.setenv("CHRONICLER_CONFIG", str(isolated_cwd / "nope.yml"))
    pkg = isolated_cwd / "pkg" / "config"
    pkg.mkdir(parents=True)
    pkg_file = pkg / "export_config.yml"
    pkg_file.write_text("opensearch:\n  url: http://pkg\n")
    monkeypatch.setattr(
        config_discovery,
        "package_export_config_path",
        lambda: pkg_file,
    )
    path, prov = config_discovery.resolve_export_config_path(None)
    assert path == pkg_file
    assert "package" in prov


def test_cwd_export_config(isolated_cwd, monkeypatch):
    monkeypatch.delenv("CHRONICLER_CONFIG", raising=False)
    missing = isolated_cwd / "pkg" / "none.yml"
    monkeypatch.setattr(
        config_discovery,
        "package_export_config_path",
        lambda: missing,
    )
    f = isolated_cwd / "export_config.yml"
    f.write_text("x: 1\n")
    path, prov = config_discovery.resolve_export_config_path(None)
    assert path.resolve() == f.resolve()
    assert "export_config.yml" in prov


def test_cwd_config_subdir_before_chronicler_subdir(isolated_cwd, monkeypatch):
    monkeypatch.delenv("CHRONICLER_CONFIG", raising=False)
    monkeypatch.setattr(
        config_discovery,
        "package_export_config_path",
        lambda: isolated_cwd / "missing" / "export_config.yml",
    )
    (isolated_cwd / "config").mkdir()
    a = isolated_cwd / "config" / "export_config.yml"
    a.write_text("a: 1\n")
    chronicler_cfg = isolated_cwd / "chronicler" / "config"
    chronicler_cfg.mkdir(parents=True)
    b = chronicler_cfg / "export_config.yml"
    b.write_text("b: 1\n")
    path, _ = config_discovery.resolve_export_config_path(None)
    assert path.resolve() == a.resolve()


def test_none_when_nothing_found(isolated_cwd, monkeypatch):
    monkeypatch.delenv("CHRONICLER_CONFIG", raising=False)
    monkeypatch.setattr(
        config_discovery,
        "package_export_config_path",
        lambda: Path("/nonexistent/chronicler/config/export_config.yml"),
    )
    path, prov = config_discovery.resolve_export_config_path(None)
    assert path is None
    assert prov == "none"
