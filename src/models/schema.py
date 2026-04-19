"""
SQLAlchemy ORM models for the Corporate Credit Rating Data Warehouse.

Implements a star schema with:
- Dimension tables (company, sector, country, currency, methodology)
- Fact table (company_snapshot)
- Audit tables (file_upload, pipeline_run, data_quality_report)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


# ============================================================================
# DIMENSION TABLES
# ============================================================================

class DimCompany(Base):
    """
    Company dimension with SCD Type 2 temporal tracking.
    
    When a company's name changes, the old record gets valid_to set and
    is_current = False, and a new record is created with is_current = True.
    """
    __tablename__ = "dim_company"

    company_key = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(String(100), nullable=False, index=True)
    company_name = Column(String(255), nullable=False)
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow)
    valid_to = Column(DateTime, default=datetime(9999, 12, 31, 23, 59, 59))
    is_current = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    snapshots = relationship("FactCompanySnapshot", back_populates="company")

    def __repr__(self) -> str:
        return f"<DimCompany(key={self.company_key}, id='{self.company_id}', name='{self.company_name}')>"


class DimSector(Base):
    """Corporate sector classification dimension."""
    __tablename__ = "dim_sector"

    sector_key = Column(Integer, primary_key=True, autoincrement=True)
    sector_name = Column(String(255), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    snapshots = relationship("FactCompanySnapshot", back_populates="sector")

    def __repr__(self) -> str:
        return f"<DimSector(key={self.sector_key}, name='{self.sector_name}')>"


class DimCountry(Base):
    """Country of origin dimension."""
    __tablename__ = "dim_country"

    country_key = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String(255), nullable=False, unique=True)
    country_code = Column(String(10), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    snapshots = relationship("FactCompanySnapshot", back_populates="country")

    def __repr__(self) -> str:
        return f"<DimCountry(key={self.country_key}, name='{self.country_name}')>"


class DimCurrency(Base):
    """Reporting currency dimension."""
    __tablename__ = "dim_currency"

    currency_key = Column(Integer, primary_key=True, autoincrement=True)
    currency_code = Column(String(10), nullable=False, unique=True)
    currency_name = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    snapshots = relationship("FactCompanySnapshot", back_populates="currency")

    def __repr__(self) -> str:
        return f"<DimCurrency(key={self.currency_key}, code='{self.currency_code}')>"


class DimRatingMethodology(Base):
    """Rating methodology dimension."""
    __tablename__ = "dim_rating_methodology"

    methodology_key = Column(Integer, primary_key=True, autoincrement=True)
    methodology_name = Column(String(500), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<DimRatingMethodology(key={self.methodology_key}, name='{self.methodology_name}')>"


# ============================================================================
# FILE UPLOAD / AUDIT TABLE
# ============================================================================

class FileUpload(Base):
    """
    Tracks every file ingested by the pipeline.
    
    Provides data lineage from source file → database records.
    file_hash ensures idempotency (same file won't be processed twice).
    """
    __tablename__ = "file_upload"

    upload_id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=True)
    file_hash = Column(String(64), nullable=False, unique=True)
    file_size_bytes = Column(Integer, nullable=True)
    upload_timestamp = Column(DateTime, default=datetime.utcnow)
    processing_status = Column(
        String(20),
        default="pending",
        nullable=False,
    )
    records_extracted = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    snapshot = relationship("FactCompanySnapshot", back_populates="upload", uselist=False)
    quality_reports = relationship("DataQualityReport", back_populates="upload")

    def __repr__(self) -> str:
        return f"<FileUpload(id={self.upload_id}, file='{self.filename}', status='{self.processing_status}')>"


# ============================================================================
# FACT TABLE
# ============================================================================

class FactCompanySnapshot(Base):
    """
    Central fact table — one row per file extraction.
    
    Captures the complete state of a company at a point in time.
    Immutable: new uploads create new rows, never overwrite.
    """
    __tablename__ = "fact_company_snapshot"

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)

    # Dimension foreign keys
    company_key = Column(Integer, ForeignKey("dim_company.company_key"), nullable=False)
    sector_key = Column(Integer, ForeignKey("dim_sector.sector_key"), nullable=True)
    country_key = Column(Integer, ForeignKey("dim_country.country_key"), nullable=True)
    currency_key = Column(Integer, ForeignKey("dim_currency.currency_key"), nullable=True)

    # Upload lineage
    upload_id = Column(Integer, ForeignKey("file_upload.upload_id"), nullable=False, unique=True)

    # Version tracking
    version_number = Column(Integer, nullable=False)

    # Rating methodologies (array of strings for multi-methodology support)
    rating_methodologies = Column(ARRAY(Text), nullable=True)

    # Industry risk data (JSONB for multi-industry weighting)
    industry_risks = Column(JSONB, nullable=True)
    segmentation_criteria = Column(String(255), nullable=True)

    # Business metadata
    accounting_principles = Column(String(50), nullable=True)
    year_end_month = Column(String(20), nullable=True)

    # Business risk profile ratings
    business_risk_profile = Column(String(20), nullable=True)
    blended_industry_risk_profile = Column(String(20), nullable=True)
    competitive_positioning = Column(String(20), nullable=True)
    market_share = Column(String(20), nullable=True)
    diversification = Column(String(20), nullable=True)
    operating_profitability = Column(String(20), nullable=True)
    sector_specific_factor_1 = Column(String(20), nullable=True)
    sector_specific_factor_2 = Column(String(20), nullable=True)

    # Financial risk profile ratings
    financial_risk_profile = Column(String(20), nullable=True)
    leverage = Column(String(20), nullable=True)
    interest_cover = Column(String(20), nullable=True)
    cash_flow_cover = Column(String(20), nullable=True)
    liquidity = Column(String(30), nullable=True)

    # Credit metrics time-series
    credit_metrics = Column(JSONB, nullable=True)

    # Temporal tracking
    snapshot_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    effective_from = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Data quality
    quality_score = Column(Float, nullable=True)
    quality_issues = Column(JSONB, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    company = relationship("DimCompany", back_populates="snapshots")
    sector = relationship("DimSector", back_populates="snapshots")
    country = relationship("DimCountry", back_populates="snapshots")
    currency = relationship("DimCurrency", back_populates="snapshots")
    upload = relationship("FileUpload", back_populates="snapshot")

    def __repr__(self) -> str:
        return (
            f"<FactCompanySnapshot(id={self.snapshot_id}, company_key={self.company_key}, "
            f"version={self.version_number})>"
        )


# ============================================================================
# PIPELINE STATE
# ============================================================================

class PipelineRun(Base):
    """Tracks each pipeline execution for monitoring and state management."""
    __tablename__ = "pipeline_run"

    run_id = Column(String(36), primary_key=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="running")
    files_found = Column(Integer, default=0)
    files_processed = Column(Integer, default=0)
    files_skipped = Column(Integer, default=0)
    files_failed = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    execution_time_ms = Column(Integer, nullable=True)
    quality_summary = Column(JSONB, nullable=True)
    error_details = Column(JSONB, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    quality_reports = relationship("DataQualityReport", back_populates="pipeline_run")

    def __repr__(self) -> str:
        return f"<PipelineRun(id='{self.run_id}', status='{self.status}')>"


# ============================================================================
# DATA QUALITY REPORT
# ============================================================================

class DataQualityReport(Base):
    """Per-file data quality assessment generated during pipeline execution."""
    __tablename__ = "data_quality_report"

    report_id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), ForeignKey("pipeline_run.run_id"), nullable=True)
    upload_id = Column(Integer, ForeignKey("file_upload.upload_id"), nullable=True)
    filename = Column(String(255), nullable=False)
    completeness_pct = Column(Float, nullable=True)
    validity_pct = Column(Float, nullable=True)
    total_fields = Column(Integer, nullable=True)
    missing_fields = Column(Integer, nullable=True)
    invalid_fields = Column(Integer, nullable=True)
    warnings = Column(JSONB, nullable=True)
    errors = Column(JSONB, nullable=True)
    field_details = Column(JSONB, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    pipeline_run = relationship("PipelineRun", back_populates="quality_reports")
    upload = relationship("FileUpload", back_populates="quality_reports")

    def __repr__(self) -> str:
        return f"<DataQualityReport(id={self.report_id}, file='{self.filename}')>"
