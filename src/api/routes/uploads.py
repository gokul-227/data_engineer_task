"""
Upload / Audit API endpoints.

Provides:
- GET /uploads - List all file uploads
- GET /uploads/stats - Upload statistics
- GET /uploads/{upload_id}/details - Upload details
- GET /uploads/{upload_id}/file - Download original file
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.database import get_db
from src.models.pydantic_models import (
    PipelineRunSchema,
    QualityReportSchema,
    UploadDetail,
    UploadStats,
    UploadSummary,
)
from src.models.schema import (
    DataQualityReport,
    FactCompanySnapshot,
    FileUpload,
    PipelineRun,
    DimCompany,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/uploads", tags=["Uploads & Audit"])


@router.get("", response_model=list[UploadSummary], summary="List all file uploads")
def list_uploads(db: Session = Depends(get_db)):
    """
    List all file uploads with metadata.
    
    Supports requirement #1 (historical tracking of all rating submissions).
    """
    uploads = (
        db.query(FileUpload)
        .order_by(FileUpload.upload_timestamp.desc())
        .all()
    )
    
    return [
        UploadSummary(
            upload_id=u.upload_id,
            filename=u.filename,
            file_hash=u.file_hash,
            file_size_bytes=u.file_size_bytes,
            upload_timestamp=u.upload_timestamp,
            processing_status=u.processing_status,
            records_extracted=u.records_extracted,
            processing_duration_ms=u.processing_duration_ms,
        )
        for u in uploads
    ]


@router.get("/stats", response_model=UploadStats, summary="Get upload statistics")
def get_upload_stats(db: Session = Depends(get_db)):
    """Get aggregated upload statistics and metrics."""
    total = db.query(func.count(FileUpload.upload_id)).scalar() or 0
    completed = db.query(func.count(FileUpload.upload_id)).filter(FileUpload.processing_status == "completed").scalar() or 0
    failed = db.query(func.count(FileUpload.upload_id)).filter(FileUpload.processing_status == "failed").scalar() or 0
    skipped = db.query(func.count(FileUpload.upload_id)).filter(FileUpload.processing_status == "skipped").scalar() or 0
    pending = db.query(func.count(FileUpload.upload_id)).filter(FileUpload.processing_status == "pending").scalar() or 0
    
    total_records = db.query(func.sum(FileUpload.records_extracted)).scalar() or 0
    avg_duration = db.query(func.avg(FileUpload.processing_duration_ms)).filter(
        FileUpload.processing_duration_ms.isnot(None)
    ).scalar()
    
    latest = db.query(func.max(FileUpload.upload_timestamp)).scalar()
    
    return UploadStats(
        total_uploads=total,
        completed=completed,
        failed=failed,
        skipped=skipped,
        pending=pending,
        total_records_extracted=total_records,
        avg_processing_duration_ms=round(avg_duration, 2) if avg_duration else None,
        latest_upload=latest,
    )


@router.get("/{upload_id}/details", response_model=UploadDetail, summary="Get upload details")
def get_upload_details(upload_id: int, db: Session = Depends(get_db)):
    """Get detailed information for a specific file upload."""
    upload = db.query(FileUpload).filter_by(upload_id=upload_id).first()
    
    if not upload:
        raise HTTPException(status_code=404, detail=f"Upload {upload_id} not found")
    
    # Get associated snapshot info
    snapshot = db.query(FactCompanySnapshot).filter_by(upload_id=upload_id).first()
    company_name = None
    quality_score = None
    
    if snapshot:
        company = db.query(DimCompany).filter_by(company_key=snapshot.company_key, is_current=True).first()
        company_name = company.company_name if company else None
        quality_score = snapshot.quality_score
    
    return UploadDetail(
        upload_id=upload.upload_id,
        filename=upload.filename,
        file_path=upload.file_path,
        file_hash=upload.file_hash,
        file_size_bytes=upload.file_size_bytes,
        upload_timestamp=upload.upload_timestamp,
        processing_status=upload.processing_status,
        records_extracted=upload.records_extracted,
        processing_duration_ms=upload.processing_duration_ms,
        error_message=upload.error_message,
        snapshot_id=snapshot.snapshot_id if snapshot else None,
        company_name=company_name,
        quality_score=quality_score,
    )


@router.get("/{upload_id}/file", summary="Download original file")
def download_upload_file(upload_id: int, db: Session = Depends(get_db)):
    """
    Download the original Excel file for a specific upload.
    
    Supports requirement #1 (file retrieval for audit).
    """
    upload = db.query(FileUpload).filter_by(upload_id=upload_id).first()
    
    if not upload:
        raise HTTPException(status_code=404, detail=f"Upload {upload_id} not found")
    
    if not upload.file_path:
        raise HTTPException(status_code=404, detail="File path not recorded")
    
    file_path = Path(upload.file_path)
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found on disk: {upload.filename}")
    
    return FileResponse(
        path=str(file_path),
        filename=upload.filename,
        media_type="application/vnd.ms-excel.sheet.macroEnabled.12",
    )


# ============================================================================
# PIPELINE RUN ENDPOINTS
# ============================================================================

@router.get("/pipeline/runs", response_model=list[PipelineRunSchema], summary="List pipeline runs",
            tags=["Pipeline"])
def list_pipeline_runs(db: Session = Depends(get_db)):
    """List all pipeline execution runs."""
    runs = (
        db.query(PipelineRun)
        .order_by(PipelineRun.started_at.desc())
        .all()
    )
    
    return [
        PipelineRunSchema.model_validate(r)
        for r in runs
    ]


@router.get("/pipeline/quality-reports", response_model=list[QualityReportSchema],
            summary="List quality reports", tags=["Pipeline"])
def list_quality_reports(db: Session = Depends(get_db)):
    """List all data quality reports."""
    reports = (
        db.query(DataQualityReport)
        .order_by(DataQualityReport.created_at.desc())
        .all()
    )
    
    return [
        QualityReportSchema.model_validate(r)
        for r in reports
    ]
