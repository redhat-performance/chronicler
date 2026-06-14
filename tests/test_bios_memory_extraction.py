"""
Unit tests for BIOS and enhanced memory metadata extraction.

Tests the extraction of:
- BIOS firmware information (vendor, version, release date) from dmidecode
- Memory type (DDR4, DDR5, etc.) from dmidecode
- Memory speed (MHz) from dmidecode
"""

import pytest
from pathlib import Path
import tempfile
import shutil

pytestmark = pytest.mark.unit

from chronicler.utils.metadata_extractor import MetadataExtractor


class TestBIOSExtraction:
    """Tests for BIOS firmware information extraction from dmidecode."""

    @pytest.fixture
    def temp_sysconfig_dir(self):
        """Create temporary sysconfig directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    def test_extract_bios_info_from_dmidecode(self, temp_sysconfig_dir):
        """Test extraction of BIOS vendor, version, and release date."""
        # Create mock dmidecode.out with BIOS information
        dmidecode_content = """# dmidecode 3.3
Getting SMBIOS data from sysfs.
SMBIOS 3.3.0 present.

Handle 0x0000, DMI type 0, 26 bytes
BIOS Information
	Vendor: American Megatrends Inc.
	Version: 1.20.07
	Release Date: 10/25/2023
	Address: 0xF0000
	Runtime Size: 64 kB
	ROM Size: 16 MB
	Characteristics:
		PCI is supported
		BIOS is upgradeable
		BIOS shadowing is allowed
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        bios_info = extractor._extract_bios_info()

        assert bios_info is not None
        assert bios_info['vendor'] == "American Megatrends Inc."
        assert bios_info['version'] == "1.20.07"
        assert bios_info['release_date'] == "10/25/2023"

    def test_extract_bios_info_missing_file(self, temp_sysconfig_dir):
        """Test BIOS extraction when dmidecode.out doesn't exist."""
        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        bios_info = extractor._extract_bios_info()

        assert bios_info is None

    def test_extract_bios_info_partial_data(self, temp_sysconfig_dir):
        """Test BIOS extraction with incomplete dmidecode output."""
        dmidecode_content = """# dmidecode 3.3
Handle 0x0000, DMI type 0, 26 bytes
BIOS Information
	Vendor: Dell Inc.
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        bios_info = extractor._extract_bios_info()

        assert bios_info is not None
        assert bios_info['vendor'] == "Dell Inc."
        assert 'version' not in bios_info
        assert 'release_date' not in bios_info


class TestMemoryTypeSpeedExtraction:
    """Tests for memory type and speed extraction from dmidecode."""

    @pytest.fixture
    def temp_sysconfig_dir(self):
        """Create temporary sysconfig directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    def test_extract_memory_type_speed_from_dmidecode(self, temp_sysconfig_dir):
        """Test extraction of memory type and speed from dmidecode."""
        dmidecode_content = """# dmidecode 3.3
Handle 0x0037, DMI type 17, 92 bytes
Memory Device
	Array Handle: 0x0036
	Error Information Handle: Not Provided
	Total Width: 72 bits
	Data Width: 64 bits
	Size: 32 GB
	Form Factor: DIMM
	Set: None
	Locator: DIMM_A0
	Bank Locator: P0_Node0_Channel0_Dimm0
	Type: DDR4
	Type Detail: Synchronous Registered (Buffered)
	Speed: 3200 MT/s
	Manufacturer: Samsung
	Serial Number: 12345678
	Asset Tag: Not Specified
	Part Number: M393A4K40DB3-CWE
	Rank: 2
	Configured Memory Speed: 3200 MT/s
	Minimum Voltage: 1.2 V
	Maximum Voltage: 1.2 V
	Configured Voltage: 1.2 V

Handle 0x003A, DMI type 17, 92 bytes
Memory Device
	Array Handle: 0x0036
	Size: 32 GB
	Type: DDR4
	Speed: 3200 MT/s
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        memory_info = extractor._extract_memory_info()

        assert memory_info is not None
        assert memory_info['type'] == "DDR4"
        assert memory_info['speed_mhz'] == 3200

    def test_extract_memory_ddr5(self, temp_sysconfig_dir):
        """Test extraction of DDR5 memory type."""
        dmidecode_content = """# dmidecode 3.3
Handle 0x0040, DMI type 17, 92 bytes
Memory Device
	Size: 64 GB
	Type: DDR5
	Speed: 4800 MT/s
	Configured Memory Speed: 4800 MT/s
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        memory_info = extractor._extract_memory_info()

        assert memory_info['type'] == "DDR5"
        assert memory_info['speed_mhz'] == 4800

    def test_extract_memory_fallback_to_configured_speed(self, temp_sysconfig_dir):
        """Test fallback to configured speed when speed field missing."""
        dmidecode_content = """# dmidecode 3.3
Handle 0x0040, DMI type 17, 92 bytes
Memory Device
	Size: 16 GB
	Type: DDR4
	Configured Memory Speed: 2666 MT/s
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        memory_info = extractor._extract_memory_info()

        assert memory_info['type'] == "DDR4"
        assert memory_info['speed_mhz'] == 2666

    def test_memory_extraction_preserves_existing_fields(self, temp_sysconfig_dir):
        """Test that dmidecode extraction doesn't overwrite proc_meminfo fields."""
        # Create proc_meminfo.out with total memory
        meminfo_content = """MemTotal:       65756928 kB
MemFree:        32456320 kB
MemAvailable:   45678912 kB
"""
        meminfo_file = temp_sysconfig_dir / "proc_meminfo.out"
        meminfo_file.write_text(meminfo_content)

        # Create dmidecode.out with type and speed
        dmidecode_content = """# dmidecode 3.3
Handle 0x0040, DMI type 17, 92 bytes
Memory Device
	Size: 64 GB
	Type: DDR4
	Speed: 3200 MT/s
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        memory_info = extractor._extract_memory_info()

        # Should have both proc_meminfo and dmidecode fields
        assert memory_info['total_kb'] == 65756928
        assert memory_info['available_kb'] == 45678912
        assert memory_info['type'] == "DDR4"
        assert memory_info['speed_mhz'] == 3200


class TestHardwareMetadataIntegration:
    """Integration tests for BIOS info in hardware metadata."""

    @pytest.fixture
    def temp_sysconfig_dir(self):
        """Create temporary sysconfig directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    def test_bios_included_in_hardware_metadata(self, temp_sysconfig_dir):
        """Test that BIOS info is included in extract_hardware_metadata."""
        dmidecode_content = """# dmidecode 3.3
Handle 0x0000, DMI type 0, 26 bytes
BIOS Information
	Vendor: Dell Inc.
	Version: 2.10.0
	Release Date: 05/15/2024
"""
        dmidecode_file = temp_sysconfig_dir / "dmidecode.out"
        dmidecode_file.write_text(dmidecode_content)

        extractor = MetadataExtractor(str(temp_sysconfig_dir))
        hardware = extractor.extract_hardware_metadata()

        assert 'bios' in hardware
        assert hardware['bios']['vendor'] == "Dell Inc."
        assert hardware['bios']['version'] == "2.10.0"
        assert hardware['bios']['release_date'] == "05/15/2024"
