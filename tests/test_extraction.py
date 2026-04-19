"""
Unit tests for Excel MASTER sheet extraction.

Tests the parser against real .xlsm files in the data/ directory.
"""

import os
import pytest
from pathlib import Path

from src.extraction.excel_parser import (
    parse_master_sheet,
    extract_all_files,
    compute_file_hash,
    extract_version_from_filename,
)

DATA_DIR = Path(__file__).parent.parent / "data"


# ============================================================================
# FILE UTILITY TESTS
# ============================================================================

class TestFileUtilities:
    """Tests for file utility functions."""

    def test_compute_file_hash_deterministic(self):
        """Same file should always produce the same hash."""
        files = list(DATA_DIR.glob("*.xlsm"))
        if not files:
            pytest.skip("No .xlsm files in data/")
        
        hash1 = compute_file_hash(files[0])
        hash2 = compute_file_hash(files[0])
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex digest length

    def test_compute_file_hash_different_files(self):
        """Different files should produce different hashes."""
        files = sorted(DATA_DIR.glob("*.xlsm"))
        if len(files) < 2:
            pytest.skip("Need at least 2 .xlsm files")
        
        hash1 = compute_file_hash(files[0])
        hash2 = compute_file_hash(files[1])
        assert hash1 != hash2

    def test_extract_version_from_filename_A1(self):
        """Test version extraction for corporates_A_1.xlsm."""
        company_id, version = extract_version_from_filename("corporates_A_1.xlsm")
        assert company_id == "company_A"
        assert version == 1

    def test_extract_version_from_filename_B2(self):
        """Test version extraction for corporates_B_2.xlsm."""
        company_id, version = extract_version_from_filename("corporates_B_2.xlsm")
        assert company_id == "company_B"
        assert version == 2


# ============================================================================
# MASTER SHEET PARSING TESTS
# ============================================================================

class TestParserCompanyA1:
    """Tests for parsing corporates_A_1.xlsm."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Parse the A_1 file once for all tests in this class."""
        file_path = DATA_DIR / "corporates_A_1.xlsm"
        if not file_path.exists():
            pytest.skip("corporates_A_1.xlsm not found")
        self.data = parse_master_sheet(file_path)

    def test_company_name(self):
        assert self.data["company_name"] == "Company A"

    def test_sector(self):
        assert self.data["sector"] == "Personal & Household Goods"

    def test_currency(self):
        assert self.data["currency"] == "EUR"

    def test_country(self):
        assert self.data["country"] == "Federal Republic of Germany"

    def test_accounting_principles(self):
        assert self.data["accounting_principles"] == "IFRS"

    def test_year_end_month(self):
        assert self.data["year_end_month"] == "December"

    def test_rating_methodologies(self):
        methodologies = self.data["rating_methodologies"]
        assert len(methodologies) >= 1
        assert "General Corporate Rating Methodology" in methodologies

    def test_industry_risks(self):
        """Company A has a single industry with weight 1.0."""
        risks = self.data["industry_risks"]
        assert len(risks) >= 1
        assert risks[0]["industry"] == "Consumer Products: Non-Discretionary"
        assert risks[0]["score"] == "A"
        assert risks[0]["weight"] == 1.0

    def test_industry_risk_weights_sum(self):
        """Weights should sum to 1.0."""
        risks = self.data["industry_risks"]
        total = sum(r["weight"] for r in risks)
        assert abs(total - 1.0) < 0.01

    def test_business_risk_profile(self):
        assert self.data["business_risk_profile"] == "B+"

    def test_financial_risk_profile(self):
        assert self.data["financial_risk_profile"] == "C"

    def test_credit_metrics_present(self):
        metrics = self.data["credit_metrics"]
        assert len(metrics) > 0
        assert "Scope-adjusted debt/EBITDA" in metrics

    def test_metadata(self):
        meta = self.data["_metadata"]
        assert meta["source_file"] == "corporates_A_1.xlsm"
        assert meta["company_id"] == "company_A"
        assert meta["version"] == 1
        assert meta["sheet_name"] == "MASTER"
        assert len(meta["file_hash"]) == 64


class TestParserCompanyA2:
    """Tests for parsing corporates_A_2.xlsm — version 2 of Company A."""

    @pytest.fixture(autouse=True)
    def setup(self):
        file_path = DATA_DIR / "corporates_A_2.xlsm"
        if not file_path.exists():
            pytest.skip("corporates_A_2.xlsm not found")
        self.data = parse_master_sheet(file_path)

    def test_same_company_name(self):
        """A_2 should be the same company as A_1."""
        assert self.data["company_name"] == "Company A"

    def test_industry_risk_score_changed(self):
        """A_1 has score 'A', A_2 should have 'BBB' (per business context)."""
        risks = self.data["industry_risks"]
        assert risks[0]["score"] == "BBB"

    def test_version_is_2(self):
        assert self.data["_metadata"]["version"] == 2


class TestParserCompanyB:
    """Tests for parsing Company B files — multi-industry risk."""

    @pytest.fixture(autouse=True)
    def setup(self):
        file_path = DATA_DIR / "corporates_B_1.xlsm"
        if not file_path.exists():
            pytest.skip("corporates_B_1.xlsm not found")
        self.data = parse_master_sheet(file_path)

    def test_company_name(self):
        assert self.data["company_name"] == "Company B"

    def test_sector(self):
        assert self.data["sector"] == "Automobiles & Parts"

    def test_currency_chf(self):
        assert self.data["currency"] == "CHF"

    def test_country_swiss(self):
        assert self.data["country"] == "Swiss Confederation"

    def test_multi_industry_risks(self):
        """Company B has TWO industries with different weights."""
        risks = self.data["industry_risks"]
        assert len(risks) == 2
        assert risks[0]["industry"] == "Automotive Suppliers"
        assert risks[1]["industry"] == "Automotive and Commercial Vehicle Manufacturers"

    def test_multi_industry_weights_sum(self):
        """Weights should sum to 1.0 even with multiple industries."""
        risks = self.data["industry_risks"]
        total = sum(r["weight"] for r in risks)
        assert abs(total - 1.0) < 0.01

    def test_b1_weights(self):
        """B_1 has weights 0.15 and 0.85."""
        risks = self.data["industry_risks"]
        assert risks[0]["weight"] == pytest.approx(0.15)
        assert risks[1]["weight"] == pytest.approx(0.85)


class TestParserCompanyB2Weights:
    """Tests that B_2 has different weights than B_1."""

    @pytest.fixture(autouse=True)
    def setup(self):
        file_path = DATA_DIR / "corporates_B_2.xlsm"
        if not file_path.exists():
            pytest.skip("corporates_B_2.xlsm not found")
        self.data = parse_master_sheet(file_path)

    def test_b2_weights_changed(self):
        """B_2 has weights 0.25 and 0.75 (different from B_1)."""
        risks = self.data["industry_risks"]
        assert risks[0]["weight"] == pytest.approx(0.25)
        assert risks[1]["weight"] == pytest.approx(0.75)


# ============================================================================
# BATCH EXTRACTION TESTS
# ============================================================================

class TestBatchExtraction:
    """Tests for extracting all files at once."""

    def test_extract_all_files(self):
        """Should extract all 4 files successfully."""
        results = extract_all_files(DATA_DIR)
        assert len(results) == 4

    def test_no_extraction_errors(self):
        """No files should have extraction errors."""
        results = extract_all_files(DATA_DIR)
        for r in results:
            assert "_extraction_error" not in r, f"Error in {r['_metadata']['source_file']}"

    def test_extract_nonexistent_dir(self):
        """Should raise when directory doesn't exist."""
        with pytest.raises(FileNotFoundError):
            extract_all_files("/nonexistent/path")
