"""
Unit tests for data quality assessment.
"""

import pytest
from pathlib import Path

from src.extraction.excel_parser import parse_master_sheet
from src.extraction.data_quality import assess_quality

DATA_DIR = Path(__file__).parent.parent / "data"


class TestDataQuality:
    """Tests for the data quality assessment module."""

    def test_quality_report_structure(self):
        """Quality report should have all expected fields."""
        file_path = DATA_DIR / "corporates_A_1.xlsm"
        if not file_path.exists():
            pytest.skip("Test data not available")
        
        data = parse_master_sheet(file_path)
        report = assess_quality(data)
        
        assert "filename" in report
        assert "completeness_pct" in report
        assert "validity_pct" in report
        assert "quality_score" in report
        assert "errors" in report
        assert "warnings" in report
        assert "field_details" in report

    def test_quality_score_range(self):
        """Quality score should be between 0 and 1."""
        file_path = DATA_DIR / "corporates_A_1.xlsm"
        if not file_path.exists():
            pytest.skip("Test data not available")
        
        data = parse_master_sheet(file_path)
        report = assess_quality(data)
        
        assert 0 <= report["quality_score"] <= 1

    def test_completeness_for_valid_file(self):
        """A valid file should have high completeness."""
        file_path = DATA_DIR / "corporates_A_1.xlsm"
        if not file_path.exists():
            pytest.skip("Test data not available")
        
        data = parse_master_sheet(file_path)
        report = assess_quality(data)
        
        assert report["completeness_pct"] >= 80

    def test_all_files_pass_quality(self):
        """All 4 test files should have quality score > 0.5."""
        for fname in ["corporates_A_1.xlsm", "corporates_A_2.xlsm",
                       "corporates_B_1.xlsm", "corporates_B_2.xlsm"]:
            file_path = DATA_DIR / fname
            if not file_path.exists():
                pytest.skip(f"{fname} not available")
            
            data = parse_master_sheet(file_path)
            report = assess_quality(data)
            
            assert report["quality_score"] > 0.5, (
                f"{fname} quality score too low: {report['quality_score']}"
            )

    def test_industry_weight_validation(self):
        """Files with correct weights should not have weight errors."""
        file_path = DATA_DIR / "corporates_B_1.xlsm"
        if not file_path.exists():
            pytest.skip("Test data not available")
        
        data = parse_master_sheet(file_path)
        report = assess_quality(data)
        
        weight_errors = [e for e in report["errors"] if "weight" in e.lower()]
        assert len(weight_errors) == 0, f"Unexpected weight errors: {weight_errors}"

    def test_missing_field_detection(self):
        """Quality report should detect missing fields."""
        data = {
            "company_name": "Test",
            "sector": None,  # Missing
            "currency": "EUR",
            "country": None,  # Missing
            "industry_risks": [],
            "rating_methodologies": [],
            "_metadata": {"source_file": "test.xlsm"},
        }
        report = assess_quality(data)
        
        assert report["missing_fields"] > 0
        assert len(report["errors"]) > 0
