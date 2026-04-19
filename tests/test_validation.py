"""
Unit tests for the data validation framework.
"""

import pytest

from src.etl.validator import validate_extracted_data, validate_batch, ValidationResult


# ============================================================================
# HELPER: Minimal valid record
# ============================================================================

def make_valid_record(**overrides) -> dict:
    """Create a minimal valid extracted data record."""
    record = {
        "company_name": "Test Company",
        "sector": "Technology",
        "currency": "EUR",
        "country": "Germany",
        "accounting_principles": "IFRS",
        "year_end_month": "December",
        "rating_methodologies": ["General Corporate Rating Methodology"],
        "industry_risks": [
            {"industry": "Software", "score": "A", "weight": 1.0}
        ],
        "business_risk_profile": "BBB",
        "financial_risk_profile": "BB+",
        "blended_industry_risk_profile": "A",
        "competitive_positioning": "A-",
        "market_share": "BBB+",
        "diversification": "A-",
        "operating_profitability": "BB+",
        "leverage": "BB+",
        "interest_cover": "A-",
        "cash_flow_cover": "A-",
        "liquidity": "+1 notch",
        "credit_metrics": {
            "Scope-adjusted debt/EBITDA": {"2020": 3.5, "2021": 3.2}
        },
        "_metadata": {
            "source_file": "test.xlsm",
            "file_hash": "abc123def456" * 5 + "abcd",
            "company_id": "company_test",
            "version": 1,
        },
    }
    record.update(overrides)
    return record


# ============================================================================
# VALIDATION TESTS
# ============================================================================

class TestValidationResult:
    """Tests for the ValidationResult dataclass."""

    def test_default_is_valid(self):
        result = ValidationResult()
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_add_error_invalidates(self):
        result = ValidationResult()
        result.add_error("Something wrong")
        assert result.is_valid is False
        assert len(result.errors) == 1

    def test_add_warning_stays_valid(self):
        result = ValidationResult()
        result.add_warning("Minor issue")
        assert result.is_valid is True
        assert len(result.warnings) == 1

    def test_to_dict(self):
        result = ValidationResult()
        result.add_error("err")
        result.add_warning("warn")
        d = result.to_dict()
        assert d["is_valid"] is False
        assert d["error_count"] == 1
        assert d["warning_count"] == 1


class TestValidateExtractedData:
    """Tests for validate_extracted_data function."""

    def test_valid_record_passes(self):
        record = make_valid_record()
        result = validate_extracted_data(record)
        assert result.is_valid is True

    def test_missing_company_name(self):
        record = make_valid_record(company_name=None)
        result = validate_extracted_data(record)
        assert result.is_valid is False
        assert any("company_name" in e for e in result.errors)

    def test_missing_sector(self):
        record = make_valid_record(sector=None)
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_missing_currency(self):
        record = make_valid_record(currency=None)
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_missing_country(self):
        record = make_valid_record(country=None)
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_invalid_company_name_too_short(self):
        record = make_valid_record(company_name="X")
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_weights_not_summing_to_one(self):
        record = make_valid_record(
            industry_risks=[
                {"industry": "A", "score": "A", "weight": 0.3},
                {"industry": "B", "score": "B", "weight": 0.3},
            ]
        )
        result = validate_extracted_data(record)
        assert result.is_valid is False
        assert any("weight" in e.lower() for e in result.errors)

    def test_weights_summing_to_one(self):
        record = make_valid_record(
            industry_risks=[
                {"industry": "A", "score": "A", "weight": 0.4},
                {"industry": "B", "score": "BB", "weight": 0.6},
            ]
        )
        result = validate_extracted_data(record)
        assert result.is_valid is True

    def test_weight_out_of_range(self):
        record = make_valid_record(
            industry_risks=[
                {"industry": "A", "score": "A", "weight": 1.5},
            ]
        )
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_extraction_error_record(self):
        record = {"_extraction_error": "File corrupt", "_metadata": {"source_file": "bad.xlsm"}}
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_missing_file_hash(self):
        record = make_valid_record()
        record["_metadata"]["file_hash"] = ""
        result = validate_extracted_data(record)
        assert result.is_valid is False

    def test_non_standard_rating_code_warning(self):
        record = make_valid_record(business_risk_profile="XYZ")
        result = validate_extracted_data(record)
        # Should warn but not error
        assert result.is_valid is True
        assert len(result.warnings) > 0

    def test_notch_adjustment_allowed(self):
        record = make_valid_record(liquidity="+1 notch")
        result = validate_extracted_data(record)
        assert result.is_valid is True


class TestBatchValidation:
    """Tests for batch validation."""

    def test_batch_validate(self):
        records = [make_valid_record(), make_valid_record(company_name=None)]
        results = validate_batch(records)
        assert len(results) == 2
        assert results[0][1].is_valid is True
        assert results[1][1].is_valid is False
