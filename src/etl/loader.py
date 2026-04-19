"""
Database loader module.

Handles loading transformed data into the PostgreSQL warehouse:
- Creates file_upload audit records
- Loads fact snapshots within transactions
- Stores data quality reports
- Implements idempotency (duplicate file detection)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.models.schema import (
    DataQualityReport,
    FactCompanySnapshot,
    FileUpload,
)

logger = logging.getLogger(__name__)


def check_file_already_processed(session: Session, file_hash: str) -> Optional[FileUpload]:
    """
    Check if a file has already been processed (idempotency).
    
    Args:
        session: Active database session
        file_hash: SHA-256 hash of the file
        
    Returns:
        FileUpload record if already processed, None otherwise
    """
    existing = (
        session.query(FileUpload)
        .filter_by(file_hash=file_hash)
        .first()
    )
    return existing


def create_upload_record(
    session: Session,
    filename: str,
    file_path: str,
    file_hash: str,
    file_size_bytes: int,
) -> FileUpload:
    """
    Create a file upload audit record.
    
    Args:
        session: Active database session
        filename: Name of the source file
        file_path: Full path to the source file
        file_hash: SHA-256 hash
        file_size_bytes: File size in bytes
        
    Returns:
        Created FileUpload record
    """
    upload = FileUpload(
        filename=filename,
        file_path=file_path,
        file_hash=file_hash,
        file_size_bytes=file_size_bytes,
        upload_timestamp=datetime.utcnow(),
        processing_status="processing",
    )
    session.add(upload)
    session.flush()  # Get the upload_id
    
    logger.info("Created upload record: id=%d, file=%s", upload.upload_id, filename)
    return upload


def load_snapshot(
    session: Session,
    snapshot: FactCompanySnapshot,
) -> FactCompanySnapshot:
    """
    Load a company snapshot into the fact table.
    
    Args:
        session: Active database session
        snapshot: Prepared snapshot ORM instance
        
    Returns:
        The loaded snapshot (with populated snapshot_id)
    """
    session.add(snapshot)
    session.flush()
    
    logger.info(
        "Loaded snapshot: id=%d, company_key=%d, version=%d",
        snapshot.snapshot_id,
        snapshot.company_key,
        snapshot.version_number,
    )
    return snapshot


def store_quality_report(
    session: Session,
    quality_report: dict[str, Any],
    run_id: str,
    upload_id: Optional[int] = None,
) -> DataQualityReport:
    """
    Store a data quality report in the database.
    
    Args:
        session: Active database session
        quality_report: Quality assessment dictionary
        run_id: Pipeline run ID
        upload_id: Associated upload ID (if available)
        
    Returns:
        Created DataQualityReport record
    """
    report = DataQualityReport(
        run_id=run_id,
        upload_id=upload_id,
        filename=quality_report.get("filename", "unknown"),
        completeness_pct=quality_report.get("completeness_pct"),
        validity_pct=quality_report.get("validity_pct"),
        total_fields=quality_report.get("total_fields_checked"),
        missing_fields=quality_report.get("missing_fields"),
        invalid_fields=quality_report.get("invalid_fields"),
        warnings=quality_report.get("warnings"),
        errors=quality_report.get("errors"),
        field_details=quality_report.get("field_details"),
    )
    session.add(report)
    session.flush()
    
    logger.info(
        "Stored quality report: id=%d, file=%s, score=%.2f%%",
        report.report_id,
        quality_report.get("filename"),
        quality_report.get("completeness_pct", 0),
    )
    return report


def mark_upload_completed(
    session: Session,
    upload: FileUpload,
    records_extracted: int = 1,
    duration_ms: Optional[int] = None,
) -> None:
    """Mark a file upload as successfully completed."""
    upload.processing_status = "completed"
    upload.records_extracted = records_extracted
    upload.processing_duration_ms = duration_ms
    session.flush()


def mark_upload_failed(
    session: Session,
    upload: FileUpload,
    error_message: str,
    duration_ms: Optional[int] = None,
) -> None:
    """Mark a file upload as failed."""
    upload.processing_status = "failed"
    upload.error_message = error_message
    upload.processing_duration_ms = duration_ms
    session.flush()
