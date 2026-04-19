"""
Snapshot API endpoints.

Provides:
- GET /snapshots - List all snapshots with filters
- GET /snapshots/latest - Latest snapshot per company
- GET /snapshots/{snapshot_id} - Specific snapshot details
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.models.database import get_db
from src.models.pydantic_models import SnapshotDetail, SnapshotLatest, SnapshotSummary
from src.models.schema import (
    DimCompany,
    DimCountry,
    DimCurrency,
    DimSector,
    FactCompanySnapshot,
    FileUpload,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/snapshots", tags=["Snapshots"])


@router.get("", response_model=list[SnapshotSummary], summary="List all snapshots with filters")
def list_snapshots(
    company_id: Optional[str] = Query(None, description="Filter by company ID"),
    from_date: Optional[datetime] = Query(None, description="Filter snapshots from this date"),
    to_date: Optional[datetime] = Query(None, description="Filter snapshots until this date"),
    sector: Optional[str] = Query(None, description="Filter by sector name"),
    country: Optional[str] = Query(None, description="Filter by country name"),
    currency: Optional[str] = Query(None, description="Filter by currency code"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db),
):
    """
    List all company snapshots with optional filters.
    
    Supports filtering by company, date range, sector, country, and currency.
    This is the primary BI-friendly endpoint (requirement #8).
    """
    query = (
        db.query(FactCompanySnapshot, DimCompany, DimSector, DimCountry, DimCurrency, FileUpload)
        .join(DimCompany, FactCompanySnapshot.company_key == DimCompany.company_key)
        .outerjoin(DimSector, FactCompanySnapshot.sector_key == DimSector.sector_key)
        .outerjoin(DimCountry, FactCompanySnapshot.country_key == DimCountry.country_key)
        .outerjoin(DimCurrency, FactCompanySnapshot.currency_key == DimCurrency.currency_key)
        .outerjoin(FileUpload, FactCompanySnapshot.upload_id == FileUpload.upload_id)
        .filter(DimCompany.is_current == True)
    )
    
    # Apply filters
    if company_id:
        query = query.filter(DimCompany.company_id == company_id)
    if from_date:
        query = query.filter(FactCompanySnapshot.snapshot_date >= from_date)
    if to_date:
        query = query.filter(FactCompanySnapshot.snapshot_date <= to_date)
    if sector:
        query = query.filter(DimSector.sector_name.ilike(f"%{sector}%"))
    if country:
        query = query.filter(DimCountry.country_name.ilike(f"%{country}%"))
    if currency:
        query = query.filter(DimCurrency.currency_code == currency.upper())
    
    results = (
        query
        .order_by(FactCompanySnapshot.snapshot_date.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    
    return [
        SnapshotSummary(
            snapshot_id=snap.snapshot_id,
            company_id=comp.company_id,
            company_name=comp.company_name,
            version_number=snap.version_number,
            sector=sec.sector_name if sec else None,
            country=co.country_name if co else None,
            currency=cur.currency_code if cur else None,
            business_risk_profile=snap.business_risk_profile,
            financial_risk_profile=snap.financial_risk_profile,
            quality_score=snap.quality_score,
            snapshot_date=snap.snapshot_date,
            source_file=upl.filename if upl else None,
        )
        for snap, comp, sec, co, cur, upl in results
    ]


@router.get("/latest", response_model=list[SnapshotLatest], summary="Latest snapshot per company")
def get_latest_snapshots(db: Session = Depends(get_db)):
    """
    Get the latest snapshot for each company.
    
    Returns one row per company with their most recent version.
    """
    from sqlalchemy import func
    
    # Subquery: max version per company
    max_version_sq = (
        db.query(
            FactCompanySnapshot.company_key,
            func.max(FactCompanySnapshot.version_number).label("max_version"),
        )
        .group_by(FactCompanySnapshot.company_key)
        .subquery()
    )
    
    results = (
        db.query(FactCompanySnapshot, DimCompany, DimSector, DimCountry, DimCurrency, FileUpload)
        .join(DimCompany, FactCompanySnapshot.company_key == DimCompany.company_key)
        .join(
            max_version_sq,
            (FactCompanySnapshot.company_key == max_version_sq.c.company_key) &
            (FactCompanySnapshot.version_number == max_version_sq.c.max_version),
        )
        .outerjoin(DimSector, FactCompanySnapshot.sector_key == DimSector.sector_key)
        .outerjoin(DimCountry, FactCompanySnapshot.country_key == DimCountry.country_key)
        .outerjoin(DimCurrency, FactCompanySnapshot.currency_key == DimCurrency.currency_key)
        .outerjoin(FileUpload, FactCompanySnapshot.upload_id == FileUpload.upload_id)
        .filter(DimCompany.is_current == True)
        .all()
    )
    
    return [
        SnapshotLatest(
            snapshot_id=snap.snapshot_id,
            company_id=comp.company_id,
            company_name=comp.company_name,
            version_number=snap.version_number,
            sector=sec.sector_name if sec else None,
            country=co.country_name if co else None,
            currency=cur.currency_code if cur else None,
            business_risk_profile=snap.business_risk_profile,
            financial_risk_profile=snap.financial_risk_profile,
            snapshot_date=snap.snapshot_date,
            source_file=upl.filename if upl else None,
        )
        for snap, comp, sec, co, cur, upl in results
    ]


@router.get("/{snapshot_id}", response_model=SnapshotDetail, summary="Get snapshot details")
def get_snapshot(snapshot_id: int, db: Session = Depends(get_db)):
    """Get full details for a specific snapshot."""
    snapshot = db.query(FactCompanySnapshot).filter_by(snapshot_id=snapshot_id).first()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")
    
    company = db.query(DimCompany).filter_by(company_key=snapshot.company_key, is_current=True).first()
    sector = db.query(DimSector).filter_by(sector_key=snapshot.sector_key).first() if snapshot.sector_key else None
    country = db.query(DimCountry).filter_by(country_key=snapshot.country_key).first() if snapshot.country_key else None
    currency = db.query(DimCurrency).filter_by(currency_key=snapshot.currency_key).first() if snapshot.currency_key else None
    upload = db.query(FileUpload).filter_by(upload_id=snapshot.upload_id).first()
    
    return SnapshotDetail(
        snapshot_id=snapshot.snapshot_id,
        company_id=company.company_id if company else "unknown",
        company_name=company.company_name if company else "Unknown",
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
