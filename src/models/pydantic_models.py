"""
Pydantic models for API request/response validation.

Provides typed, validated schemas for all API endpoints with
proper serialization and OpenAPI documentation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ============================================================================
# DIMENSION SCHEMAS
# ============================================================================

class SectorSchema(BaseModel):
    """Sector dimension."""
    sector_key: int
    sector_name: str
    
    class Config:
        from_attributes = True


class CountrySchema(BaseModel):
    """Country dimension."""
    country_key: int
    country_name: str
    country_code: Optional[str] = None
    
    class Config:
        from_attributes = True


class CurrencySchema(BaseModel):
    """Currency dimension."""
    currency_key: int
    currency_code: str
    currency_name: Optional[str] = None
    
    class Config:
        from_attributes = True


class MethodologySchema(BaseModel):
    """Rating methodology dimension."""
    methodology_key: int
    methodology_name: str
    
    class Config:
        from_attributes = True


# ============================================================================
# COMPANY SCHEMAS
# ============================================================================

class CompanyBasic(BaseModel):
    """Basic company info for list endpoints."""
    company_id: str
    company_name: str
    sector: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    latest_version: Optional[int] = None
    total_versions: Optional[int] = None
    latest_snapshot_date: Optional[datetime] = None


class CompanyDetail(BaseModel):
    """Full company details (latest version)."""
    company_id: str
    company_name: str
    sector: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    version_number: int
    rating_methodologies: Optional[list[str]] = None
    industry_risks: Optional[list[dict[str, Any]]] = None
    segmentation_criteria: Optional[str] = None
    accounting_principles: Optional[str] = None
    year_end_month: Optional[str] = None
    business_risk_profile: Optional[str] = None
    blended_industry_risk_profile: Optional[str] = None
    competitive_positioning: Optional[str] = None
    market_share: Optional[str] = None
    diversification: Optional[str] = None
    operating_profitability: Optional[str] = None
    sector_specific_factor_1: Optional[str] = None
    sector_specific_factor_2: Optional[str] = None
    financial_risk_profile: Optional[str] = None
    leverage: Optional[str] = None
    interest_cover: Optional[str] = None
    cash_flow_cover: Optional[str] = None
    liquidity: Optional[str] = None
    credit_metrics: Optional[dict[str, Any]] = None
    quality_score: Optional[float] = None
    snapshot_date: Optional[datetime] = None
    source_file: Optional[str] = None


class CompanyVersion(BaseModel):
    """Version entry for company version history."""
    version_number: int
    snapshot_id: int
    snapshot_date: datetime
    source_file: Optional[str] = None
    quality_score: Optional[float] = None
    business_risk_profile: Optional[str] = None
    financial_risk_profile: Optional[str] = None
    industry_risks: Optional[list[dict[str, Any]]] = None


class CompanyHistory(BaseModel):
    """Time-series history entry for a company."""
    version_number: int
    snapshot_date: datetime
    business_risk_profile: Optional[str] = None
    financial_risk_profile: Optional[str] = None
    blended_industry_risk_profile: Optional[str] = None
    leverage: Optional[str] = None
    interest_cover: Optional[str] = None
    cash_flow_cover: Optional[str] = None
    liquidity: Optional[str] = None
    competitive_positioning: Optional[str] = None
    market_share: Optional[str] = None
    diversification: Optional[str] = None
    operating_profitability: Optional[str] = None
    industry_risks: Optional[list[dict[str, Any]]] = None
    credit_metrics: Optional[dict[str, Any]] = None
    source_file: Optional[str] = None


class CompanyCompare(BaseModel):
    """Company comparison at a point in time."""
    company_id: str
    company_name: str
    version_number: int
    snapshot_date: datetime
    sector: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    business_risk_profile: Optional[str] = None
    financial_risk_profile: Optional[str] = None
    blended_industry_risk_profile: Optional[str] = None
    leverage: Optional[str] = None
    industry_risks: Optional[list[dict[str, Any]]] = None


# ============================================================================
# SNAPSHOT SCHEMAS
# ============================================================================

class SnapshotSummary(BaseModel):
    """Snapshot summary for list endpoints."""
    snapshot_id: int
    company_id: str
    company_name: str
    version_number: int
    sector: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    business_risk_profile: Optional[str] = None
    financial_risk_profile: Optional[str] = None
    quality_score: Optional[float] = None
    snapshot_date: datetime
    source_file: Optional[str] = None


class SnapshotDetail(CompanyDetail):
    """Full snapshot details (extends CompanyDetail with snapshot_id)."""
    snapshot_id: int


class SnapshotLatest(BaseModel):
    """Latest snapshot per company."""
    snapshot_id: int
    company_id: str
    company_name: str
    version_number: int
    sector: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    business_risk_profile: Optional[str] = None
    financial_risk_profile: Optional[str] = None
    snapshot_date: datetime
    source_file: Optional[str] = None


# ============================================================================
# UPLOAD SCHEMAS
# ============================================================================

class UploadSummary(BaseModel):
    """File upload summary."""
    upload_id: int
    filename: str
    file_hash: str
    file_size_bytes: Optional[int] = None
    upload_timestamp: datetime
    processing_status: str
    records_extracted: int = 0
    processing_duration_ms: Optional[int] = None


class UploadDetail(UploadSummary):
    """Detailed file upload info."""
    file_path: Optional[str] = None
    error_message: Optional[str] = None
    snapshot_id: Optional[int] = None
    company_name: Optional[str] = None
    quality_score: Optional[float] = None


class UploadStats(BaseModel):
    """Upload statistics."""
    total_uploads: int
    completed: int
    failed: int
    skipped: int
    pending: int
    total_records_extracted: int
    avg_processing_duration_ms: Optional[float] = None
    latest_upload: Optional[datetime] = None


# ============================================================================
# PIPELINE SCHEMAS
# ============================================================================

class PipelineRunSchema(BaseModel):
    """Pipeline run information."""
    run_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    files_found: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    records_inserted: int = 0
    execution_time_ms: Optional[int] = None
    quality_summary: Optional[dict[str, Any]] = None
    
    class Config:
        from_attributes = True


# ============================================================================
# DATA QUALITY SCHEMAS
# ============================================================================

class QualityReportSchema(BaseModel):
    """Data quality report."""
    report_id: int
    filename: str
    completeness_pct: Optional[float] = None
    validity_pct: Optional[float] = None
    total_fields: Optional[int] = None
    missing_fields: Optional[int] = None
    invalid_fields: Optional[int] = None
    warnings: Optional[list[str]] = None
    errors: Optional[list[str]] = None
    field_details: Optional[dict[str, Any]] = None
    created_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# HEALTH CHECK
# ============================================================================

class HealthCheck(BaseModel):
    """API health check response."""
    status: str
    database: str
    api_version: str = "1.0.0"
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# ============================================================================
# ERROR SCHEMAS
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str
    status_code: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
