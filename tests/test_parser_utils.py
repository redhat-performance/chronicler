"""
Unit tests for chronicler.utils.parser_utils.

Tests parsing functions for various file formats.
"""

import pytest
from pathlib import Path

pytestmark = pytest.mark.unit

from chronicler.utils.parser_utils import (
    parse_csv_timeseries,
    parse_key_value_text,
    parse_proc_file,
    parse_test_times,
    parse_command_file,
    parse_simple_yaml,
    read_file_lines,
    read_file_content,
    parse_status_file,
    parse_meminfo,
    parse_version_file,
    _parse_numeric_value,
    _clean_key_name,
    _parse_command_arguments,
)


class TestParseNumericValue:
    """Tests for _parse_numeric_value helper."""

    def test_parses_integer(self):
        assert _parse_numeric_value("42") == 42
        assert _parse_numeric_value("-10") == -10
        assert _parse_numeric_value("0") == 0

    def test_parses_float(self):
        assert _parse_numeric_value("3.14") == 3.14
        assert _parse_numeric_value("-2.5") == -2.5
        assert _parse_numeric_value("0.0") == 0.0

    def test_returns_string_for_non_numeric(self):
        assert _parse_numeric_value("hello") == "hello"
        assert _parse_numeric_value("2026-03-17") == "2026-03-17"
        assert _parse_numeric_value("v1.0.0") == "v1.0.0"

    def test_strips_whitespace(self):
        assert _parse_numeric_value("  42  ") == 42
        assert _parse_numeric_value("  hello  ") == "hello"

    def test_handles_non_string_input(self):
        assert _parse_numeric_value(42) == 42
        assert _parse_numeric_value(3.14) == 3.14


class TestCleanKeyName:
    """Tests for _clean_key_name helper."""

    def test_converts_to_lowercase(self):
        assert _clean_key_name("CoreMark") == "coremark"

    def test_replaces_spaces_with_underscores(self):
        assert _clean_key_name("CoreMark Size") == "coremark_size"

    def test_removes_parentheses_content(self):
        assert _clean_key_name("Total time (secs)") == "total_time"

    def test_replaces_slash_with_per(self):
        assert _clean_key_name("Iterations/Sec") == "iterations_per_sec"

    def test_collapses_multiple_underscores(self):
        assert _clean_key_name("foo   bar") == "foo_bar"

    def test_strips_leading_trailing_underscores(self):
        assert _clean_key_name("  foo  ") == "foo"


class TestParseCommandArguments:
    """Tests for _parse_command_arguments helper."""

    def test_parses_simple_arguments(self):
        cmd = "/path/to/script --arg1 value1 --arg2 value2"
        args = _parse_command_arguments(cmd)
        assert args == {"arg1": "value1", "arg2": "value2"}

    def test_strips_quotes_from_values(self):
        cmd = '/path/to/script --name "test"'
        args = _parse_command_arguments(cmd)
        assert args == {"name": "test"}

    def test_handles_empty_command(self):
        assert _parse_command_arguments("") == {}

    def test_handles_no_arguments(self):
        cmd = "/path/to/script"
        assert _parse_command_arguments(cmd) == {}


class TestParseCsvTimeseries:
    """Tests for parse_csv_timeseries function."""

    def test_parses_colon_delimited_csv(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("iteration:threads:value\n1:4:100.5\n2:4:200.5\n")

        data = parse_csv_timeseries(str(csv_file), delimiter=":")
        assert len(data) == 2
        assert data[0]["iteration"] == 1
        assert data[0]["threads"] == 4
        assert data[0]["value"] == 100.5

    def test_parses_comma_delimited_csv(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6\n")

        data = parse_csv_timeseries(str(csv_file), delimiter=",")
        assert len(data) == 2
        assert data[0] == {"a": 1, "b": 2, "c": 3}

    def test_skips_comments_when_enabled(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("# comment\na,b\n1,2\n")

        data = parse_csv_timeseries(str(csv_file), delimiter=",", skip_comments=True)
        assert len(data) == 1
        assert data[0] == {"a": 1, "b": 2}

    def test_returns_empty_for_missing_file(self, tmp_path):
        data = parse_csv_timeseries(str(tmp_path / "nonexistent.csv"))
        assert data == []

    def test_preserves_string_values(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,date\ntest,2026-03-17\n")

        data = parse_csv_timeseries(str(csv_file), delimiter=",")
        assert data[0]["name"] == "test"
        assert data[0]["date"] == "2026-03-17"


class TestParseKeyValueText:
    """Tests for parse_key_value_text function."""

    def test_parses_key_value_pairs(self, tmp_path):
        file = tmp_path / "config.txt"
        file.write_text("CoreMark Size: 666\nIterations/Sec: 195999.82\n")

        data = parse_key_value_text(str(file))
        assert data["coremark_size"] == 666
        assert data["iterations_per_sec"] == 195999.82

    def test_handles_custom_separator(self, tmp_path):
        file = tmp_path / "config.txt"
        file.write_text("key=value\ncount=42\n")

        data = parse_key_value_text(str(file), separator="=")
        assert data["key"] == "value"
        assert data["count"] == 42

    def test_skips_empty_lines(self, tmp_path):
        file = tmp_path / "config.txt"
        file.write_text("key1: value1\n\nkey2: value2\n")

        data = parse_key_value_text(str(file))
        assert len(data) == 2

    def test_returns_empty_for_missing_file(self, tmp_path):
        data = parse_key_value_text(str(tmp_path / "nonexistent.txt"))
        assert data == {}


class TestParseProcFile:
    """Tests for parse_proc_file function."""

    def test_parses_cpuinfo_style(self, tmp_path):
        file = tmp_path / "cpuinfo"
        file.write_text("processor: 0\nvendor_id: GenuineIntel\ncpu family: 6\n")

        data = parse_proc_file(str(file))
        assert data["processor"] == "0"
        assert data["vendor_id"] == "GenuineIntel"
        assert data["cpu_family"] == "6"

    def test_handles_duplicate_keys_as_list(self, tmp_path):
        file = tmp_path / "cpuinfo"
        file.write_text("processor: 0\nprocessor: 1\nprocessor: 2\n")

        data = parse_proc_file(str(file))
        assert data["processor"] == ["0", "1", "2"]

    def test_returns_empty_for_missing_file(self, tmp_path):
        data = parse_proc_file(str(tmp_path / "nonexistent"))
        assert data == {}


class TestParseTestTimes:
    """Tests for parse_test_times function."""

    def test_parses_test_times(self, tmp_path):
        file = tmp_path / "test_times"
        file.write_text(
            "test: streams execution time 2\n"
            "test: coremark execution time 204\n"
            "test: pig execution time 519\n"
        )

        data = parse_test_times(str(file))
        assert len(data) == 3
        assert data[0] == {"test": "streams", "execution_time_seconds": 2}
        assert data[1] == {"test": "coremark", "execution_time_seconds": 204}

    def test_skips_non_test_lines(self, tmp_path):
        file = tmp_path / "test_times"
        file.write_text("some other line\ntest: foo execution time 10\n")

        data = parse_test_times(str(file))
        assert len(data) == 1

    def test_returns_empty_for_missing_file(self, tmp_path):
        data = parse_test_times(str(tmp_path / "nonexistent"))
        assert data == []


class TestParseCommandFile:
    """Tests for parse_command_file function."""

    def test_parses_command_with_shebang(self, tmp_path):
        file = tmp_path / "test.cmd"
        file.write_text("#!/bin/bash\n/path/to/script --iterations 5\n")

        data = parse_command_file(str(file))
        assert data["shebang"] == "#!/bin/bash"
        assert data["command"] == "/path/to/script --iterations 5"
        assert data["arguments"] == {"iterations": "5"}

    def test_parses_command_without_shebang(self, tmp_path):
        file = tmp_path / "test.cmd"
        file.write_text("/path/to/script --arg value\n")

        data = parse_command_file(str(file))
        assert "shebang" not in data
        assert data["command"] == "/path/to/script --arg value"

    def test_returns_empty_for_missing_file(self, tmp_path):
        data = parse_command_file(str(tmp_path / "nonexistent.cmd"))
        assert data == {}


class TestReadFileLines:
    """Tests for read_file_lines function."""

    def test_reads_lines_stripped(self, tmp_path):
        file = tmp_path / "test.txt"
        file.write_text("line1\n  line2  \nline3\n")

        lines = read_file_lines(str(file))
        assert lines == ["line1", "line2", "line3"]

    def test_reads_lines_unstripped(self, tmp_path):
        file = tmp_path / "test.txt"
        file.write_text("line1\n  line2  \n")

        lines = read_file_lines(str(file), strip=False)
        assert lines == ["line1\n", "  line2  \n"]

    def test_returns_empty_for_missing_file(self, tmp_path):
        lines = read_file_lines(str(tmp_path / "nonexistent.txt"))
        assert lines == []


class TestReadFileContent:
    """Tests for read_file_content function."""

    def test_reads_full_content(self, tmp_path):
        file = tmp_path / "test.txt"
        file.write_text("hello world")

        content = read_file_content(str(file))
        assert content == "hello world"

    def test_returns_empty_for_missing_file(self, tmp_path):
        content = read_file_content(str(tmp_path / "nonexistent.txt"))
        assert content == ""


class TestParseStatusFile:
    """Tests for parse_status_file function."""

    @pytest.mark.parametrize(
        "content,expected",
        [
            ("Ran", "PASS"),
            ("PASS", "PASS"),
            ("passed", "PASS"),
            ("SUCCESS", "PASS"),
            ("FAIL", "FAIL"),
            ("failed", "FAIL"),
            ("ERROR", "FAIL"),
            ("UNKNOWN_STATUS", "UNKNOWN_STATUS"),
            ("", "UNKNOWN"),
        ],
    )
    def test_maps_status_values(self, tmp_path, content, expected):
        file = tmp_path / "status"
        file.write_text(content)

        status = parse_status_file(str(file))
        assert status == expected


class TestParseMeminfo:
    """Tests for parse_meminfo function."""

    def test_parses_meminfo(self, tmp_path):
        file = tmp_path / "meminfo"
        file.write_text("MemTotal:       16328328 kB\nMemFree:        13445772 kB\n")

        data = parse_meminfo(str(file))
        assert data["memtotal"] == 16328328
        assert data["memfree"] == 13445772

    def test_returns_empty_for_missing_file(self, tmp_path):
        data = parse_meminfo(str(tmp_path / "nonexistent"))
        assert data == {}


class TestParseVersionFile:
    """Tests for parse_version_file function."""

    def test_extracts_version_after_commit(self, tmp_path):
        file = tmp_path / "version"
        file.write_text("commit: v1.01")

        version = parse_version_file(str(file))
        assert version == "v1.01"

    def test_returns_raw_content_if_no_commit(self, tmp_path):
        file = tmp_path / "version"
        file.write_text("1.0.0")

        version = parse_version_file(str(file))
        assert version == "1.0.0"

    def test_returns_empty_for_missing_file(self, tmp_path):
        version = parse_version_file(str(tmp_path / "nonexistent"))
        assert version == ""


class TestParseSimpleYaml:
    """Tests for parse_simple_yaml function."""

    def test_parses_simple_yaml(self, tmp_path):
        file = tmp_path / "config.yaml"
        file.write_text("name: test\ncount: 42\n")

        data = parse_simple_yaml(str(file))
        assert data["name"] == "test"
        assert data["count"] == 42
