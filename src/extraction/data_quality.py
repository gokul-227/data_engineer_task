"""
Data quality assessment module for extracted corporate credit rating data.

Generates per-file quality reports with:
- Completeness metrics (% of required fields present)
- Validity metrics (% of values passing type/range checks)
- Specific warnings and errors per field
- Overall quality score
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# ============================================================================
# FIELD DEFINITIONS FOR QUALITY CHECKS
# ============================================================================

REQUIRED_FIELDS = [
    "company_name",
    "sector",
    "currency",
    "country",
    "accounting_principles",
    "year_end_month",
]

RECOMMENDED_FIELDS = [
    "business_risk_profile",
    "financial_risk_profile",
    "blended_industry_risk_profile",
    "competitive_positioning",
    "leverage",
    "interest_cover",
    "cash_flow_cover",
    "liquidity",
    "segmentation_criteria",
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

VALID_MONTHS = {
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
}

VALID_ACCOUNTING_PRINCIPLES = {"IFRS", "US GAAP", "GAAP", "Local GAAP"}

VALID_CURRENCIES = {"EUR", "USD", "GBP", "CHF", "JPY", "CNY", "SEK", "NOK", "DKK"}


def assess_quality(extracted_data: dict[str, Any]) -> dict[str, Any]:
    """
    Assess data quality of extracted data from a single file.
    
    Args:
        extracted_data: Dictionary from excel_parser.parse_master_sheet()
        
    Returns:
        Quality report dictionary with scores, warnings, and errors
    """
    metadata = extracted_data.get("_metadata", {})
    filename = metadata.get("source_file", "unknown")
    
    warnings: list[str] = []
    errors: list[str] = []
    field_details: dict[str, dict[str, Any]] = {}
    
    total_fields = 0
    present_fields = 0
    valid_fields = 0
    
    # ---- Check required fields ----
    for field in REQUIRED_FIELDS:
        total_fields += 1
        value = extracted_data.get(field)
        detail = {"required": True, "present": False, "valid": False, "value": None}
        
        if value is not None and str(value).strip():
            present_fields += 1
            detail["present"] = True
            detail["value"] = str(value)
            
            # Type-specific validation
            is_valid = True
            if field == "year_end_month" and str(value) not in VALID_MONTHS:
                warnings.append(f"Unexpected year_end_month value: '{value}'")
                is_valid = False
            elif field == "accounting_principles" and str(value) not in VALID_ACCOUNTING_PRINCIPLES:
                warnings.append(f"Unexpected accounting_principles value: '{value}'")
                is_valid = False
            elif field == "currency" and str(value) not in VALID_CURRENCIES:
                warnings.append(f"Unexpected currency value: '{value}'")
                is_valid = False
            
            if is_valid:
                valid_fields += 1
                detail["valid"] = True
        else:
            errors.append(f"Required field missing: '{field}'")
        
        field_details[field] = detail
    
    # ---- Check recommended fields ----
    for field in RECOMMENDED_FIELDS:
        total_fields += 1
        value = extracted_data.get(field)
        detail = {"required": False, "present": False, "valid": False, "value": None}
        
        if value is not None and str(value).strip():
            present_fields += 1
            detail["present"] = True
            detail["value"] = str(value)
            valid_fields += 1
            detail["valid"] = True
        else:
            warnings.append(f"Recommended field missing: '{field}'")
        
        field_details[field] = detail
    
    # ---- Validate rating codes ----
    for field in RATING_FIELDS:
        value = extracted_data.get(field)
        if value is not None and str(value).strip():
            if str(value) not in VALID_RATING_CODES:
                # Allow notch adjustments like "+1 notch"
                if "notch" not in str(value).lower():
                    warnings.append(
                        f"Non-standard rating code for '{field}': '{value}'"
                    )
    
    # ---- Validate industry risk data ----
    industry_risks = extracted_data.get("industry_risks", [])
    if industry_risks:
        total_weight = sum(r.get("weight", 0) for r in industry_risks if r.get("weight") is not None)
        
        if abs(total_weight - 1.0) > 0.01:
            errors.append(
                f"Industry risk weights sum to {total_weight:.4f}, expected ~1.0"
            )
        else:
            field_details["industry_risk_weights_sum"] = {
                "required": True,
                "present": True,
                "valid": True,
                "value": f"{total_weight:.4f}",
            }
        
        for i, risk in enumerate(industry_risks):
            if "industry" not in risk:
                warnings.append(f"Industry risk entry {i}: missing industry name")
            if "score" not in risk:
                warnings.append(f"Industry risk entry {i}: missing score")
            if "weight" not in risk:
                errors.append(f"Industry risk entry {i}: missing weight")
    else:
        errors.append("No industry risk data found")
    
    # ---- Validate rating methodologies ----
    methodologies = extracted_data.get("rating_methodologies", [])
    if not methodologies:
        errors.append("No rating methodologies found")
    else:
        field_details["rating_methodologies"] = {
            "required": True,
            "present": True,
            "valid": True,
            "value": "; ".join(methodologies),
        }
        total_fields += 1
        present_fields += 1
        valid_fields += 1
    
    # ---- Validate credit metrics ----
    credit_metrics = extracted_data.get("credit_metrics", {})
    if credit_metrics:
        for metric_name, year_data in credit_metrics.items():
            null_years = [yr for yr, val in year_data.items() if val is None]
            if null_years:
                warnings.append(
                    f"Credit metric '{metric_name}' has missing data for years: {null_years}"
                )
    else:
        warnings.append("No credit metrics data found")
    
    # ---- Compute scores ----
    completeness_pct = (present_fields / total_fields * 100) if total_fields > 0 else 0
    validity_pct = (valid_fields / total_fields * 100) if total_fields > 0 else 0
    
    # Overall quality score (weighted: 60% completeness, 30% validity, 10% no errors)
    error_penalty = max(0, 1 - len(errors) * 0.1)
    quality_score = (0.6 * completeness_pct / 100 + 0.3 * validity_pct / 100 + 0.1 * error_penalty)
    quality_score = round(min(1.0, max(0.0, quality_score)), 4)
    
    report = {
        "filename": filename,
        "assessed_at": datetime.utcnow().isoformat(),
        "completeness_pct": round(completeness_pct, 2),
        "validity_pct": round(validity_pct, 2),
        "quality_score": quality_score,
        "total_fields_checked": total_fields,
        "present_fields": present_fields,
        "missing_fields": total_fields - present_fields,
        "valid_fields": valid_fields,
        "invalid_fields": total_fields - valid_fields,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "field_details": field_details,
    }
    
    logger.info(
        "Quality report for %s: completeness=%.1f%%, validity=%.1f%%, score=%.4f, errors=%d, warnings=%d",
        filename,
        completeness_pct,
        validity_pct,
        quality_score,
        len(errors),
        len(warnings),
    )
    
    return report
