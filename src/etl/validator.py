"""
Data validation framework for the ETL pipeline.

Validates extracted data before loading into the warehouse:
- Required field presence
- Data type validation
- Numeric range validation (weights, scores)
- Business rule validation
- Anomaly detection
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validating a single extracted record."""
    
    is_valid: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    
    def add_error(self, message: str) -> None:
        """Add a validation error (blocks loading)."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a validation warning (non-blocking)."""
        self.warnings.append(message)
    
    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings,
        }


# ============================================================================
# VALIDATION RULES
# ============================================================================

REQUIRED_FIELDS = [
    "company_name",
    "sector",
    "currency",
    "country",
]

VALID_RATING_CODES = {
    "AAA", "AA+", "AA", "AA-",
    "A+", "A", "A-",
    "BBB+", "BBB", "BBB-",
    "BB+", "BB", "BB-",
    "B+", "B", "B-",
    "CCC+", "CCC", "CCC-",
    "CC", "C", "D",
}

RATING_FIELDS = [
    "business_risk_profile",
    "blended_industry_risk_profile",
    "competitive_positioning",
    "market_share",
    "diversification",
    "operating_profitability",
    "sector_specific_factor_1",
    "sector_specific_factor_2",
    "financial_risk_profile",
    "leverage",
    "interest_cover",
    "cash_flow_cover",
]


def validate_extracted_data(data: dict[str, Any]) -> ValidationResult:
    """
    Validate a single extracted data record against all rules.
    
    Args:
        data: Extracted data dictionary from excel_parser
        
    Returns:
        ValidationResult with errors and warnings
    """
    result = ValidationResult()
    metadata = data.get("_metadata", {})
    filename = metadata.get("source_file", "unknown")
    
    # Check for extraction errors
    if "_extraction_error" in data:
        result.add_error(f"Extraction failed: {data['_extraction_error']}")
        return result
    
    # ---- Rule 1: Required fields must be present ----
    for field_name in REQUIRED_FIELDS:
        value = data.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            result.add_error(f"Required field '{field_name}' is missing or empty")
    
    # ---- Rule 2: Company name must be a non-empty string ----
    company_name = data.get("company_name")
    if company_name is not None:
        if not isinstance(company_name, str) or len(company_name.strip()) < 2:
            result.add_error(f"Invalid company_name: '{company_name}'")
    
    # ---- Rule 3: Rating codes must be valid ----
    for field_name in RATING_FIELDS:
        value = data.get(field_name)
        if value is not None and str(value).strip():
            val_str = str(value).strip()
            if val_str not in VALID_RATING_CODES and "notch" not in val_str.lower():
                result.add_warning(
                    f"Non-standard rating code for '{field_name}': '{val_str}'"
                )
    
    # ---- Rule 4: Industry risk weights must sum to ~1.0 ----
    industry_risks = data.get("industry_risks", [])
    if industry_risks:
        weights = [r.get("weight", 0) for r in industry_risks if r.get("weight") is not None]
        if weights:
            total_weight = sum(weights)
            if abs(total_weight - 1.0) > 0.05:
                result.add_error(
                    f"Industry risk weights sum to {total_weight:.4f}, expected ~1.0 (±0.05)"
                )
            elif abs(total_weight - 1.0) > 0.01:
                result.add_warning(
                    f"Industry risk weights sum to {total_weight:.4f}, slightly off from 1.0"
                )
        else:
            result.add_warning("Industry risk entries exist but no weights found")
        
        # Validate each weight is between 0 and 1
        for i, risk in enumerate(industry_risks):
            weight = risk.get("weight")
            if weight is not None:
                if not (0 <= weight <= 1):
                    result.add_error(
                        f"Industry risk weight[{i}] = {weight} is outside valid range [0, 1]"
                    )
    else:
        result.add_warning("No industry risk data found")
    
    # ---- Rule 5: Rating methodologies must exist ----
    methodologies = data.get("rating_methodologies", [])
    if not methodologies:
        result.add_warning("No rating methodologies found")
    
    # ---- Rule 6: Year-end month must be valid ----
    year_end = data.get("year_end_month")
    if year_end is not None:
        valid_months = {
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        }
        if str(year_end) not in valid_months:
            result.add_warning(f"Unexpected year_end_month: '{year_end}'")
    
    # ---- Rule 7: Credit metrics numeric validation ----
    credit_metrics = data.get("credit_metrics", {})
    for metric_name, year_data in credit_metrics.items():
        if not isinstance(year_data, dict):
            result.add_warning(f"Credit metric '{metric_name}' has invalid format")
            continue
        
        for year, value in year_data.items():
            if value is not None and not isinstance(value, (int, float)):
                result.add_warning(
                    f"Credit metric '{metric_name}' year {year} has non-numeric value: '{value}'"
                )
    
    # ---- Rule 8: Metadata integrity ----
    if not metadata.get("file_hash"):
        result.add_error("Missing file hash in metadata")
    if not metadata.get("company_id"):
        result.add_error("Missing company_id in metadata")
    
    logger.info(
        "Validation for %s: valid=%s, errors=%d, warnings=%d",
        filename,
        result.is_valid,
        len(result.errors),
        len(result.warnings),
    )
    
    return result


def validate_batch(data_list: list[dict[str, Any]]) -> list[tuple[dict[str, Any], ValidationResult]]:
    """
    Validate a batch of extracted records.
    
    Args:
        data_list: List of extracted data dictionaries
        
    Returns:
        List of (data, validation_result) tuples
    """
    results = []
    for data in data_list:
        validation = validate_extracted_data(data)
        results.append((data, validation))
    
    valid_count = sum(1 for _, v in results if v.is_valid)
    logger.info(
        "Batch validation: %d/%d records valid",
        valid_count,
        len(results),
    )
    
    return results
