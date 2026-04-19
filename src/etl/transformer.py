"""
Data transformation module.

Transforms extracted raw data into database-ready format:
- Resolves/creates dimension references (company, sector, country, currency)
- Handles SCD Type 2 for company dimension
- Assigns version numbers
- Prepares fact table records
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.schema import (
    DimCompany,
    DimCountry,
    DimCurrency,
    DimRatingMethodology,
    DimSector,
    FactCompanySnapshot,
)

logger = logging.getLogger(__name__)


def get_or_create_sector(session: Session, sector_name: str) -> DimSector:
    """Get existing sector or create new one."""
    sector = session.query(DimSector).filter_by(sector_name=sector_name).first()
    if not sector:
        sector = DimSector(sector_name=sector_name)
        session.add(sector)
        session.flush()
        logger.info("Created sector dimension: %s", sector_name)
    return sector


def get_or_create_country(session: Session, country_name: str) -> DimCountry:
    """Get existing country or create new one."""
    country = session.query(DimCountry).filter_by(country_name=country_name).first()
    if not country:
        country = DimCountry(country_name=country_name)
        session.add(country)
        session.flush()
        logger.info("Created country dimension: %s", country_name)
    return country


def get_or_create_currency(session: Session, currency_code: str) -> DimCurrency:
    """Get existing currency or create new one."""
    currency = session.query(DimCurrency).filter_by(currency_code=currency_code).first()
    if not currency:
        currency = DimCurrency(currency_code=currency_code)
        session.add(currency)
        session.flush()
        logger.info("Created currency dimension: %s", currency_code)
    return currency


def get_or_create_methodology(session: Session, methodology_name: str) -> DimRatingMethodology:
    """Get existing methodology or create new one."""
    meth = session.query(DimRatingMethodology).filter_by(methodology_name=methodology_name).first()
    if not meth:
        meth = DimRatingMethodology(methodology_name=methodology_name)
        session.add(meth)
        session.flush()
        logger.info("Created methodology dimension: %s", methodology_name)
    return meth


def get_or_create_company(session: Session, company_id: str, company_name: str) -> DimCompany:
    """
    Get or create company dimension with SCD Type 2 handling.
    
    If the company exists with the same name, returns it.
    If the company exists with a different name, closes the old record
    and creates a new current record (SCD Type 2).
    If the company doesn't exist, creates a new record.
    """
    current = (
        session.query(DimCompany)
        .filter_by(company_id=company_id, is_current=True)
        .first()
    )
    
    if current:
        if current.company_name == company_name:
            # Same name — no SCD change needed
            return current
        else:
            # Name changed — SCD Type 2: close old record, create new
            logger.info(
                "SCD Type 2: Company '%s' name changed from '%s' to '%s'",
                company_id, current.company_name, company_name,
            )
            now = datetime.utcnow()
            current.valid_to = now
            current.is_current = False
            
            new_company = DimCompany(
                company_id=company_id,
                company_name=company_name,
                valid_from=now,
                is_current=True,
            )
            session.add(new_company)
            session.flush()
            return new_company
    else:
        # New company
        company = DimCompany(
            company_id=company_id,
            company_name=company_name,
            is_current=True,
        )
        session.add(company)
        session.flush()
        logger.info("Created company dimension: %s (%s)", company_id, company_name)
        return company


def get_next_version_number(session: Session, company_key: int) -> int:
    """Get the next version number for a company."""
    max_version = (
        session.query(func.max(FactCompanySnapshot.version_number))
        .filter_by(company_key=company_key)
        .scalar()
    )
    return (max_version or 0) + 1


def transform_to_snapshot(
    session: Session,
    extracted_data: dict[str, Any],
    upload_id: int,
    quality_report: dict[str, Any],
) -> FactCompanySnapshot:
    """
    Transform extracted data into a FactCompanySnapshot database record.
    
    Resolves all dimension references, assigns version numbers,
    and creates the fact table record.
    
    Args:
        session: Active database session
        extracted_data: Data from excel_parser
        upload_id: Associated file_upload record ID
        quality_report: Quality assessment report
        
    Returns:
        FactCompanySnapshot ORM instance (not yet committed)
    """
    metadata = extracted_data.get("_metadata", {})
    
    # ---- Resolve dimensions ----
    company_id = metadata.get("company_id", "unknown")
    company_name = extracted_data.get("company_name", "Unknown Company")
    company = get_or_create_company(session, company_id, company_name)
    
    sector = None
    if extracted_data.get("sector"):
        sector = get_or_create_sector(session, extracted_data["sector"])
    
    country = None
    if extracted_data.get("country"):
        country = get_or_create_country(session, extracted_data["country"])
    
    currency = None
    if extracted_data.get("currency"):
        currency = get_or_create_currency(session, extracted_data["currency"])
    
    # Ensure methodologies exist in dimension table
    for meth_name in extracted_data.get("rating_methodologies", []):
        get_or_create_methodology(session, meth_name)
    
    # ---- Version number ----
    version_number = get_next_version_number(session, company.company_key)
    
    # ---- Build snapshot record ----
    now = datetime.utcnow()
    
    snapshot = FactCompanySnapshot(
        company_key=company.company_key,
        sector_key=sector.sector_key if sector else None,
        country_key=country.country_key if country else None,
        currency_key=currency.currency_key if currency else None,
        upload_id=upload_id,
        version_number=version_number,
        rating_methodologies=extracted_data.get("rating_methodologies", []),
        industry_risks=extracted_data.get("industry_risks", []),
        segmentation_criteria=extracted_data.get("segmentation_criteria"),
        accounting_principles=extracted_data.get("accounting_principles"),
        year_end_month=extracted_data.get("year_end_month"),
        business_risk_profile=extracted_data.get("business_risk_profile"),
        blended_industry_risk_profile=extracted_data.get("blended_industry_risk_profile"),
        competitive_positioning=extracted_data.get("competitive_positioning"),
        market_share=extracted_data.get("market_share"),
        diversification=extracted_data.get("diversification"),
        operating_profitability=extracted_data.get("operating_profitability"),
        sector_specific_factor_1=extracted_data.get("sector_specific_factor_1"),
        sector_specific_factor_2=extracted_data.get("sector_specific_factor_2"),
        financial_risk_profile=extracted_data.get("financial_risk_profile"),
        leverage=extracted_data.get("leverage"),
        interest_cover=extracted_data.get("interest_cover"),
        cash_flow_cover=extracted_data.get("cash_flow_cover"),
        liquidity=extracted_data.get("liquidity"),
        credit_metrics=extracted_data.get("credit_metrics", {}),
        snapshot_date=now,
        effective_from=now,
        quality_score=quality_report.get("quality_score"),
        quality_issues={
            "errors": quality_report.get("errors", []),
            "warnings": quality_report.get("warnings", []),
        },
    )
    
    logger.info(
        "Transformed snapshot: company=%s, version=%d, upload_id=%d",
        company_id, version_number, upload_id,
    )
    
    return snapshot
