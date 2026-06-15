"""
Tests for BaseProcessor.build_test_info() method.

Tests verify wrapper version extraction from test_info file
and document the current behavior where both version and
wrapper_version fields are set to the same value.
"""

import json
import pytest
from pathlib import Path

from chronicler.processors.base_processor import BaseProcessor
from chronicler.schema import TestInfo

pytestmark = pytest.mark.unit


class MinimalProcessor(BaseProcessor):
    """Minimal concrete processor for testing BaseProcessor methods."""

    def get_test_name(self) -> str:
        return "minimal_test"

    def parse_runs(self, extracted_result):
        return {}


def test_build_test_info_extracts_wrapper_version_from_test_info(tmp_path):
    """
    Verify build_test_info() extracts wrapper version from test_info file.

    The test_info file contains wrapper repository versions (e.g., "v2.8")
    which should be extracted and set to both version and wrapper_version
    fields (current behavior).
    """
    # Setup: Create test_info file with wrapper version
    test_info_data = {
        "minimal_test": {
            "test_name": "minimal_test",
            "repo_file": "v2.8.tar.gz"
        }
    }
    test_info_file = tmp_path / "test_info"
    test_info_file.write_text(json.dumps(test_info_data))

    # Execute
    processor = MinimalProcessor(str(tmp_path))
    result = processor.build_test_info()

    # Verify
    assert isinstance(result, TestInfo)
    assert result.name == "minimal_test"
    assert result.version == "v2.8", "Should extract wrapper version from repo_file"
    assert result.wrapper_version == "v2.8", "Should set wrapper_version to same value"


def test_build_test_info_returns_unknown_when_no_test_info_file(tmp_path):
    """
    When test_info file is missing, both version fields should be "unknown".
    """
    # Execute (no test_info file created)
    processor = MinimalProcessor(str(tmp_path))
    result = processor.build_test_info()

    # Verify
    assert result.version == "unknown"
    assert result.wrapper_version == "unknown"


def test_build_test_info_returns_unknown_when_test_not_in_test_info(tmp_path):
    """
    When test_info exists but doesn't contain the test, return "unknown".
    """
    # Setup: Create test_info with different test
    test_info_data = {
        "other_test": {
            "test_name": "other_test",
            "repo_file": "v1.0.tar.gz"
        }
    }
    test_info_file = tmp_path / "test_info"
    test_info_file.write_text(json.dumps(test_info_data))

    # Execute
    processor = MinimalProcessor(str(tmp_path))
    result = processor.build_test_info()

    # Verify
    assert result.version == "unknown"
    assert result.wrapper_version == "unknown"


def test_build_test_info_handles_malformed_json(tmp_path):
    """
    Malformed test_info file should log warning and return "unknown".
    """
    # Setup: Create invalid JSON
    test_info_file = tmp_path / "test_info"
    test_info_file.write_text("{ invalid json }")

    # Execute
    processor = MinimalProcessor(str(tmp_path))
    result = processor.build_test_info()

    # Verify
    assert result.version == "unknown"
    assert result.wrapper_version == "unknown"
