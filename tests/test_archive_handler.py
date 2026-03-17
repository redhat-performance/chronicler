"""
Integration tests for chronicler.utils.archive_handler.

Tests archive extraction with programmatically created fixture files.
"""

import os
import io
import tarfile
import zipfile
import pytest
from pathlib import Path

pytestmark = pytest.mark.integration

from chronicler.utils.archive_handler import (
    ArchiveHandler,
    ArchiveExtractionError,
    extract_result,
    extract_sysconfig,
)


def create_result_archive(base_path: Path, test_name: str = "coremark") -> Path:
    """
    Create a minimal results_{test}.zip archive for testing.

    Structure:
    results_{test}.zip
    └── results_{test}_.tar
        └── {test}_2026.03.17-10.00.00/
            ├── test_results_report
            ├── results_{test}.csv
            ├── run1_summary
            ├── run2_summary
            └── version
    """
    timestamp = "2026.03.17-10.00.00"
    result_dir_name = f"{test_name}_{timestamp}"

    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        dir_info = tarfile.TarInfo(name=result_dir_name)
        dir_info.type = tarfile.DIRTYPE
        dir_info.mode = 0o755
        tar.addfile(dir_info)

        files = {
            "test_results_report": "PASS",
            f"results_{test_name}.csv": "iteration,value\n1,100\n2,200\n",
            "run1_summary": "Run 1 completed",
            "run2_summary": "Run 2 completed",
            "version": "commit: v1.0.0",
            "tuned_setting": "throughput-performance",
        }
        for filename, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=f"{result_dir_name}/{filename}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    tar_buffer.seek(0)
    tar_filename = f"results_{test_name}_.tar"

    zip_path = base_path / f"results_{test_name}.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(tar_filename, tar_buffer.read())

    return zip_path


def create_sysconfig_archive(base_path: Path) -> Path:
    """
    Create a minimal sysconfig_info.tar archive for testing.

    Structure:
    sysconfig_info.tar
    └── sysconfig_info/
        ├── lscpu.json
        ├── proc_cpuinfo.out
        ├── proc_meminfo.out
        └── uname.out
    """
    tar_path = base_path / "sysconfig_info.tar"

    with tarfile.open(tar_path, "w") as tar:
        dir_info = tarfile.TarInfo(name="sysconfig_info")
        dir_info.type = tarfile.DIRTYPE
        dir_info.mode = 0o755
        tar.addfile(dir_info)

        files = {
            "lscpu.json": '{"cpu": "Intel"}',
            "proc_cpuinfo.out": "processor: 0\nvendor_id: GenuineIntel\n",
            "proc_meminfo.out": "MemTotal: 16328328 kB\n",
            "uname.out": "Linux test 5.14.0-1 x86_64\n",
        }
        for filename, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=f"sysconfig_info/{filename}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    return tar_path


def create_boot_info_archive(base_path: Path) -> Path:
    """
    Create a minimal boot_info.tar archive for testing.
    """
    tar_path = base_path / "initial_boot_info.tar"

    with tarfile.open(tar_path, "w") as tar:
        dir_info = tarfile.TarInfo(name="boot_info")
        dir_info.type = tarfile.DIRTYPE
        dir_info.mode = 0o755
        tar.addfile(dir_info)

        files = {
            "boot.log": "Boot sequence started",
            "kernel_params": "root=/dev/sda1",
        }
        for filename, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=f"boot_info/{filename}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    return tar_path


class TestArchiveHandler:
    """Tests for ArchiveHandler class."""

    def test_extract_result_archive_success(self, tmp_path):
        zip_path = create_result_archive(tmp_path, "coremark")

        handler = ArchiveHandler()
        try:
            result = handler.extract_result_archive(str(zip_path))

            assert result["test_name"] == "coremark"
            assert os.path.isdir(result["extracted_path"])
            assert "temp_dir" in result
            assert result["files"]["test_results_report"] is not None
            assert result["files"]["results_csv"] is not None
            assert result["files"]["version"] is not None
            assert len(result["files"]["run_summaries"]) == 2
        finally:
            handler.cleanup()

    def test_extract_result_archive_file_contents(self, tmp_path):
        zip_path = create_result_archive(tmp_path, "streams")

        with ArchiveHandler() as handler:
            result = handler.extract_result_archive(str(zip_path))

            version_path = result["files"]["version"]
            assert version_path is not None
            with open(version_path) as f:
                content = f.read()
            assert "v1.0.0" in content

    def test_extract_result_archive_missing_file(self, tmp_path):
        handler = ArchiveHandler()

        with pytest.raises(ArchiveExtractionError) as exc_info:
            handler.extract_result_archive(str(tmp_path / "nonexistent.zip"))

        assert "not found" in str(exc_info.value)

    def test_extract_result_archive_invalid_zip(self, tmp_path):
        invalid_zip = tmp_path / "results_test.zip"
        invalid_zip.write_text("not a zip file")

        handler = ArchiveHandler()
        with pytest.raises(ArchiveExtractionError):
            handler.extract_result_archive(str(invalid_zip))

    def test_extract_result_archive_no_tar_inside(self, tmp_path):
        zip_path = tmp_path / "results_test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("some_file.txt", "content")

        handler = ArchiveHandler()
        with pytest.raises(ArchiveExtractionError) as exc_info:
            handler.extract_result_archive(str(zip_path))

        assert "No tar file found" in str(exc_info.value)

    def test_extract_sysconfig_archive_success(self, tmp_path):
        tar_path = create_sysconfig_archive(tmp_path)

        with ArchiveHandler() as handler:
            result = handler.extract_sysconfig_archive(str(tar_path))

            assert os.path.isdir(result["extracted_path"])
            assert result["files"]["lscpu_json"] is not None
            assert result["files"]["proc_cpuinfo"] is not None
            assert result["files"]["proc_meminfo"] is not None

    def test_extract_sysconfig_archive_missing_file(self, tmp_path):
        handler = ArchiveHandler()

        with pytest.raises(ArchiveExtractionError) as exc_info:
            handler.extract_sysconfig_archive(str(tmp_path / "nonexistent.tar"))

        assert "not found" in str(exc_info.value)

    def test_extract_boot_info_archive_success(self, tmp_path):
        tar_path = create_boot_info_archive(tmp_path)

        with ArchiveHandler() as handler:
            result = handler.extract_boot_info_archive(str(tar_path))

            assert os.path.isdir(result["extracted_path"])
            assert len(result["files"]["all_files"]) == 2

    def test_extract_boot_info_archive_missing_file(self, tmp_path):
        handler = ArchiveHandler()

        with pytest.raises(ArchiveExtractionError) as exc_info:
            handler.extract_boot_info_archive(str(tmp_path / "nonexistent.tar"))

        assert "not found" in str(exc_info.value)

    def test_context_manager_cleans_up(self, tmp_path):
        zip_path = create_result_archive(tmp_path)
        temp_dir = None

        with ArchiveHandler() as handler:
            result = handler.extract_result_archive(str(zip_path))
            temp_dir = result["temp_dir"]
            assert os.path.exists(temp_dir)

        assert not os.path.exists(temp_dir)

    def test_cleanup_removes_all_temp_dirs(self, tmp_path):
        zip_path = create_result_archive(tmp_path, "test1")
        tar_path = create_sysconfig_archive(tmp_path)

        handler = ArchiveHandler()
        result1 = handler.extract_result_archive(str(zip_path))
        result2 = handler.extract_sysconfig_archive(str(tar_path))

        temp_dirs = [result1["temp_dir"], result2["temp_dir"]]
        for d in temp_dirs:
            assert os.path.exists(d)

        handler.cleanup()

        for d in temp_dirs:
            assert not os.path.exists(d)

    def test_cleanup_on_error_true(self, tmp_path):
        invalid_zip = tmp_path / "results_test.zip"
        invalid_zip.write_text("not a zip file")

        handler = ArchiveHandler(cleanup_on_error=True)
        initial_temp_dirs_count = len(handler.temp_dirs)

        with pytest.raises(ArchiveExtractionError):
            handler.extract_result_archive(str(invalid_zip))

        assert len(handler.temp_dirs) == initial_temp_dirs_count

    def test_multiple_extractions(self, tmp_path):
        zip1 = create_result_archive(tmp_path, "test1")
        zip2 = create_result_archive(tmp_path, "test2")

        with ArchiveHandler() as handler:
            result1 = handler.extract_result_archive(str(zip1))
            result2 = handler.extract_result_archive(str(zip2))

            assert result1["test_name"] == "test1"
            assert result2["test_name"] == "test2"
            assert result1["extracted_path"] != result2["extracted_path"]


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_extract_result_success(self, tmp_path):
        zip_path = create_result_archive(tmp_path)

        result = extract_result(str(zip_path), cleanup=True)

        assert result["test_name"] == "coremark"
        assert "extracted_path" in result

    def test_extract_result_missing_file(self, tmp_path):
        with pytest.raises(ArchiveExtractionError):
            extract_result(str(tmp_path / "nonexistent.zip"))

    def test_extract_sysconfig_success(self, tmp_path):
        tar_path = create_sysconfig_archive(tmp_path)

        result = extract_sysconfig(str(tar_path), cleanup=True)

        assert "extracted_path" in result
        assert result["files"]["lscpu_json"] is not None

    def test_extract_sysconfig_missing_file(self, tmp_path):
        with pytest.raises(ArchiveExtractionError):
            extract_sysconfig(str(tmp_path / "nonexistent.tar"))


class TestFileCataloging:
    """Tests for file cataloging functionality."""

    def test_catalogs_all_files(self, tmp_path):
        zip_path = create_result_archive(tmp_path)

        with ArchiveHandler() as handler:
            result = handler.extract_result_archive(str(zip_path))

            all_files = result["files"]["all_files"]
            assert len(all_files) >= 5
            filenames = [os.path.basename(f) for f in all_files]
            assert "test_results_report" in filenames
            assert "results_coremark.csv" in filenames
            assert "version" in filenames

    def test_finds_run_summaries(self, tmp_path):
        zip_path = create_result_archive(tmp_path)

        with ArchiveHandler() as handler:
            result = handler.extract_result_archive(str(zip_path))

            summaries = result["files"]["run_summaries"]
            assert len(summaries) == 2
            assert any("run1_summary" in s for s in summaries)
            assert any("run2_summary" in s for s in summaries)

    def test_optional_files_return_none(self, tmp_path):
        tar_path = tmp_path / "sysconfig_info.tar"
        with tarfile.open(tar_path, "w") as tar:
            dir_info = tarfile.TarInfo(name="sysconfig_info")
            dir_info.type = tarfile.DIRTYPE
            dir_info.mode = 0o755
            tar.addfile(dir_info)
            data = b"minimal"
            info = tarfile.TarInfo(name="sysconfig_info/lscpu.json")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        with ArchiveHandler() as handler:
            result = handler.extract_sysconfig_archive(str(tar_path))

            assert result["files"]["lscpu_json"] is not None
            assert result["files"]["proc_cpuinfo"] is None
            assert result["files"]["dmidecode"] is None
