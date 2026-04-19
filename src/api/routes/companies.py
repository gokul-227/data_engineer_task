"""
Company API endpoints.

Provides:
- GET /companies - List all companies (latest state)
- GET /companies/{company_id} - Get latest version details
- GET /companies/{company_id}/versions - All versions (version control)
- GET /companies/{company_id}/history - Time-series evolution
- GET /companies/compare - Point-in-time multi-company comparison
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, distinct
from sqlalchemy.orm import Session

from src.models.database import get_db
from src.models.pydantic_models import (
    CompanyBasic,
    CompanyCompare,
    CompanyDetail,
    CompanyHistory,
    CompanyVersion,
)
from src.models.schema import (
    DimCompany,
    DimCountry,
    DimCurrency,
    DimSector,
    FactCompanySnapshot,
    FileUpload,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/companies", tags=["Companies"])


@router.get("", response_model=list[CompanyBasic], summary="List all companies")
def list_companies(db: Session = Depends(get_db)):
    """
    List all companies with their current metadata and latest version info.
    
    Returns unique companies with aggregated version statistics.
    """
    # Subquery: latest version per company
    latest_version_sq = (
        db.query(
            DimCompany.company_id,
            func.max(FactCompanySnapshot.version_number).label("latest_version"),
            func.count(FactCompanySnapshot.snapshot_id).label("total_versions"),
            func.max(FactCompanySnapshot.snapshot_date).label("latest_snapshot_date"),
        )
        .join(FactCompanySnapshot, DimCompany.company_key == FactCompanySnapshot.company_key)
        .filter(DimCompany.is_current == True)
        .group_by(DimCompany.company_id)
        .subquery()
    )
    
    # Main query with dimension joins
    results = (
        db.query(
            DimCompany.company_id,
            DimCompany.company_name,
            DimSector.sector_name,
            DimCountry.country_name,
            DimCurrency.currency_code,
            latest_version_sq.c.latest_version,
            latest_version_sq.c.total_versions,
            latest_version_sq.c.latest_snapshot_date,
        )
        .join(latest_version_sq, DimCompany.company_id == latest_version_sq.c.company_id)
        .outerjoin(
            FactCompanySnapshot,
            (DimCompany.company_key == FactCompanySnapshot.company_key) &
            (FactCompanySnapshot.version_number == latest_version_sq.c.latest_version),
        )
        .outerjoin(DimSector, FactCompanySnapshot.sector_key == DimSector.sector_key)
        .outerjoin(DimCountry, FactCompanySnapshot.country_key == DimCountry.country_key)
        .outerjoin(DimCurrency, FactCompanySnapshot.currency_key == DimCurrency.currency_key)
        .filter(DimCompany.is_current == True)
        .all()
    )
    
    return [
        CompanyBasic(
            company_id=r[0],
            company_name=r[1],
            sector=r[2],
            country=r[3],
            currency=r[4],
            latest_version=r[5],
            total_versions=r[6],
            latest_snapshot_date=r[7],
        )
        for r in results
    ]


@router.get("/compare", response_model=list[CompanyCompare], summary="Compare companies at a point in time")
def compare_companies(
    company_ids: str = Query(..., description="Comma-separated company IDs (e.g., 'company_A,company_B')"),
    as_of_date: Optional[datetime] = Query(None, description="Point-in-time date (ISO format). Defaults to latest."),
    db: Session = Depends(get_db),
):
    """
    Compare multiple companies at a specific point in time.
    
    For each requested company, returns the latest snapshot as of the given date.
    This supports requirement #2 (point-in-time company comparisons).
    """
    company_id_list = [cid.strip() for cid in company_ids.split(",") if cid.strip()]
    
    if not company_id_list:
        raise HTTPException(status_code=400, detail="At least one company_id is required")
    
    results = []
    for cid in company_id_list:
        # Find company
        company = (
            db.query(DimCompany)
            .filter_by(company_id=cid, is_current=True)
            .first()
        )
        
        if not company:
            continue
        
        # Get latest snapshot as of date
        query = (
            db.query(FactCompanySnapshot)
            .filter(FactCompanySnapshot.company_key == company.company_key)
        )
        
        if as_of_date:
            query = query.filter(FactCompanySnapshot.snapshot_date <= as_of_date)
        
        snapshot = query.order_by(FactCompanySnapshot.version_number.desc()).first()
        
        if snapshot:
            sector = db.query(DimSector).filter_by(sector_key=snapshot.sector_key).first()
            country = db.query(DimCountry).filter_by(country_key=snapshot.country_key).first()
            currency = db.query(DimCurrency).filter_by(currency_key=snapshot.currency_key).first()
            
            results.append(CompanyCompare(
                company_id=cid,
                company_name=company.company_name,
                version_number=snapshot.version_number,
                snapshot_date=snapshot.snapshot_date,
                sector=sector.sector_name if sector else None,
                country=country.country_name if country else None,
                currency=currency.currency_code if currency else None,
                business_risk_profile=snapshot.business_risk_profile,
                financial_risk_profile=snapshot.financial_risk_profile,
                blended_industry_risk_profile=snapshot.blended_industry_risk_profile,
                leverage=snapshot.leverage,
                industry_risks=snapshot.industry_risks,
            ))
    
    if not results:
        raise HTTPException(status_code=404, detail="No companies found for the given IDs")
    
    return results


@router.get("/{company_id}", response_model=CompanyDetail, summary="Get company details (latest version)")
def get_company(company_id: str, db: Session = Depends(get_db)):
    """
    Get full details for a company's latest version.
    
    Returns all extracted metadata, risk profiles, and credit metrics.
    """
    company = (
        db.query(DimCompany)
        .filter_by(company_id=company_id, is_current=True)
        .first()
    )
    
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")
    
    snapshot = (
        db.query(FactCompanySnapshot)
        .filter_by(company_key=company.company_key)
        .order_by(FactCompanySnapshot.version_number.desc())
        .first()
    )
    
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"No snapshots found for company '{company_id}'")
    
    sector = db.query(DimSector).filter_by(sector_key=snapshot.sector_key).first() if snapshot.sector_key else None
    country = db.query(DimCountry).filter_by(country_key=snapshot.country_key).first() if snapshot.country_key else None
    currency = db.query(DimCurrency).filter_by(currency_key=snapshot.currency_key).first() if snapshot.currency_key else None
    upload = db.query(FileUpload).filter_by(upload_id=snapshot.upload_id).first()
    
    return CompanyDetail(
        company_id=company_id,
        company_name=company.company_name,
        sector=sector.sector_name if sector else None,
        country=country.country_name if country else None,
        currency=currency.currency_code if currency else None,
        version_number=snapshot.version_number,
        rating_methodologies=snapshot.rating_methodologies,
        industry_risks=snapshot.industry_risks,
        segmentation_criteria=snapshot.segmentation_criteria,
        accounting_principles=snapshot.accounting_principles,
        year_end_month=snapshot.year_end_month,
        business_risk_profile=snapshot.business_risk_profile,
        blended_industry_risk_profile=snapshot.blended_industry_risk_profile,
        competitive_positioning=snapshot.competitive_positioning,
        market_share=snapshot.market_share,
        diversification=snapshot.diversification,
        operating_profitability=snapshot.operating_profitability,
        sector_specific_factor_1=snapshot.sector_specific_factor_1,
        sector_specific_factor_2=snapshot.sector_specific_factor_2,
        financial_risk_profile=snapshot.financial_risk_profile,
        leverage=snapshot.leverage,
        interest_cover=snapshot.interest_cover,
        cash_flow_cover=snapshot.cash_flow_cover,
        liquidity=snapshot.liquidity,
        credit_metrics=snapshot.credit_metrics,
        quality_score=snapshot.quality_score,
        snapshot_date=snapshot.snapshot_date,
        source_file=upload.filename if upload else None,
    )


@router.get("/{company_id}/versions", response_model=list[CompanyVersion], summary="Get all versions")
def get_company_versions(company_id: str, db: Session = Depends(get_db)):
    """
    Get all historical versions for a company.
    
    Supports requirement #4 (version control for multiple uploads).
    Returns versions ordered by version number descending.
    """
    company = (
        db.query(DimCompany)
        .filter_by(company_id=company_id, is_current=True)
        .first()
    )
    
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")
    
    snapshots = (
        db.query(FactCompanySnapshot, FileUpload)
        .join(FileUpload, FactCompanySnapshot.upload_id == FileUpload.upload_id)
        .filter(FactCompanySnapshot.company_key == company.company_key)
        .order_by(FactCompanySnapshot.version_number.desc())
        .all()
    )
    
    if not snapshots:
        raise HTTPException(status_code=404, detail=f"No versions found for company '{company_id}'")
    
    return [
        CompanyVersion(
            version_number=s.version_number,
            snapshot_id=s.snapshot_id,
            snapshot_date=s.snapshot_date,
            source_file=u.filename,
            quality_score=s.quality_score,
            business_risk_profile=s.business_risk_profile,
            financial_risk_profile=s.financial_risk_profile,
            industry_risks=s.industry_risks,
        )
        for s, u in snapshots
    ]


@router.get("/{company_id}/history", response_model=list[CompanyHistory], summary="Get time-series history")
def get_company_history(company_id: str, db: Session = Depends(get_db)):
    """
    Get time-series evolution data for a company.
    
    Supports requirement #3 (time-series analysis) and requirement #6 (time-series data availability).
    Returns all snapshots ordered chronologically for trend analysis.
    """
    company = (
        db.query(DimCompany)
        .filter_by(company_id=company_id, is_current=True)
        .first()
    )
    
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")
    
    snapshots = (
        db.query(FactCompanySnapshot, FileUpload)
        .join(FileUpload, FactCompanySnapshot.upload_id == FileUpload.upload_id)
        .filter(FactCompanySnapshot.company_key == company.company_key)
        .order_by(FactCompanySnapshot.version_number.asc())
        .all()
    )
    
    if not snapshots:
        raise HTTPException(status_code=404, detail=f"No history found for company '{company_id}'")
    
    return [
        CompanyHistory(
            version_number=s.version_number,
            snapshot_date=s.snapshot_date,
            business_risk_profile=s.business_risk_profile,
            financial_risk_profile=s.financial_risk_profile,
            blended_industry_risk_profile=s.blended_industry_risk_profile,
            leverage=s.leverage,
            interest_cover=s.interest_cover,
            cash_flow_cover=s.cash_flow_cover,
            liquidity=s.liquidity,
            competitive_positioning=s.competitive_positioning,
            market_share=s.market_share,
            diversification=s.diversification,
            operating_profitability=s.operating_profitability,
            industry_risks=s.industry_risks,
            credit_metrics=s.credit_metrics,
            source_file=u.filename,
        )
        for s, u in snapshots
    ]
