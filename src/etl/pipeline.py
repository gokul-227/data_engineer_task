"""
Main ETL pipeline orchestrator.

Coordinates the full Extract → Validate → Transform → Load workflow:
- Discovers files to process
- Implements idempotency (skip already-processed files)
- Manages pipeline state and execution metrics
- Handles failures with retry logic
- Generates comprehensive pipeline reports
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from src.config import get_settings
from src.extraction.data_quality import assess_quality
from src.extraction.excel_parser import compute_file_hash, extract_all_files, parse_master_sheet
from src.etl.loader import (
    check_file_already_processed,
    create_upload_record,
    load_snapshot,
    mark_upload_completed,
    mark_upload_failed,
    store_quality_report,
)
from src.etl.transformer import transform_to_snapshot
from src.etl.validator import validate_extracted_data
from src.models.database import get_session_factory, init_database
from src.models.schema import PipelineRun

logger = logging.getLogger(__name__)


class PipelineResult:
    """Tracks the result of a pipeline execution."""
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.started_at = datetime.utcnow()
        self.completed_at: datetime | None = None
        self.status = "running"
        self.files_found = 0
        self.files_processed = 0
        self.files_skipped = 0
        self.files_failed = 0
        self.records_inserted = 0
        self.quality_reports: list[dict] = []
        self.errors: list[dict] = []
        self.file_details: list[dict] = []
    
    def to_dict(self) -> dict[str, Any]:
        """Serialize pipeline result to dictionary."""
        elapsed = (
            (self.completed_at - self.started_at).total_seconds()
            if self.completed_at
            else (datetime.utcnow() - self.started_at).total_seconds()
        )
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "execution_time_seconds": round(elapsed, 3),
            "files_found": self.files_found,
            "files_processed": self.files_processed,
            "files_skipped": self.files_skipped,
            "files_failed": self.files_failed,
            "records_inserted": self.records_inserted,
            "quality_summary": {
                "avg_completeness": (
                    sum(r.get("completeness_pct", 0) for r in self.quality_reports) / len(self.quality_reports)
                    if self.quality_reports else 0
                ),
                "avg_validity": (
                    sum(r.get("validity_pct", 0) for r in self.quality_reports) / len(self.quality_reports)
                    if self.quality_reports else 0
                ),
                "total_errors": sum(r.get("error_count", 0) for r in self.quality_reports),
                "total_warnings": sum(r.get("warning_count", 0) for r in self.quality_reports),
            },
            "errors": self.errors,
            "file_details": self.file_details,
        }


def _process_single_file(
    session: Session,
    file_path: Path,
    run_id: str,
    pipeline_result: PipelineResult,
    max_retries: int = 3,
) -> bool:
    """
    Process a single Excel file through the full ETL pipeline.
    
    Implements retry with exponential backoff for transient failures.
    
    Returns True if file was successfully processed, False otherwise.
    """
    filename = file_path.name
    file_start = time.time()
    
    for attempt in range(1, max_retries + 1):
        try:
            # ---- Step 1: Check idempotency ----
            file_hash = compute_file_hash(file_path)
            existing = check_file_already_processed(session, file_hash)
            
            if existing:
                logger.info(
                    "Skipping already-processed file: %s (upload_id=%d)",
                    filename, existing.upload_id,
                )
                pipeline_result.files_skipped += 1
                pipeline_result.file_details.append({
                    "filename": filename,
                    "status": "skipped",
                    "reason": "already processed",
                    "existing_upload_id": existing.upload_id,
                })
                return True
            
            # ---- Step 2: Extract ----
            logger.info("Extracting: %s (attempt %d/%d)", filename, attempt, max_retries)
            extracted_data = parse_master_sheet(file_path)
            
            # ---- Step 3: Validate ----
            validation_result = validate_extracted_data(extracted_data)
            
            if not validation_result.is_valid:
                logger.warning(
                    "Validation failed for %s: %s",
                    filename, validation_result.errors,
                )
                # Still proceed to create upload record for audit trail,
                # but mark as failed
                upload = create_upload_record(
                    session,
                    filename=filename,
                    file_path=str(file_path.absolute()),
                    file_hash=file_hash,
                    file_size_bytes=file_path.stat().st_size,
                )
                mark_upload_failed(
                    session, upload,
                    f"Validation errors: {validation_result.errors}",
                    duration_ms=int((time.time() - file_start) * 1000),
                )
                
                pipeline_result.files_failed += 1
                pipeline_result.errors.append({
                    "filename": filename,
                    "errors": validation_result.errors,
                })
                pipeline_result.file_details.append({
                    "filename": filename,
                    "status": "failed",
                    "reason": "validation errors",
                    "errors": validation_result.errors,
                })
                session.commit()
                return False
            
            # ---- Step 4: Quality Assessment ----
            quality_report = assess_quality(extracted_data)
            pipeline_result.quality_reports.append(quality_report)
            
            # ---- Step 5: Create upload record ----
            metadata = extracted_data["_metadata"]
            upload = create_upload_record(
                session,
                filename=filename,
                file_path=str(file_path.absolute()),
                file_hash=file_hash,
                file_size_bytes=metadata.get("file_size_bytes", 0),
            )
            
            # ---- Step 6: Transform ----
            snapshot = transform_to_snapshot(
                session, extracted_data, upload.upload_id, quality_report,
            )
            
            # ---- Step 7: Load ----
            load_snapshot(session, snapshot)
            
            # ---- Step 8: Store quality report ----
            store_quality_report(session, quality_report, run_id, upload.upload_id)
            
            # ---- Step 9: Mark upload complete ----
            duration_ms = int((time.time() - file_start) * 1000)
            mark_upload_completed(session, upload, records_extracted=1, duration_ms=duration_ms)
            
            # ---- Commit transaction ----
            session.commit()
            
            pipeline_result.files_processed += 1
            pipeline_result.records_inserted += 1
            pipeline_result.file_details.append({
                "filename": filename,
                "status": "completed",
                "upload_id": upload.upload_id,
                "snapshot_id": snapshot.snapshot_id,
                "version_number": snapshot.version_number,
                "company_name": extracted_data.get("company_name"),
                "quality_score": quality_report.get("quality_score"),
                "duration_ms": duration_ms,
            })
            
            logger.info(
                "Successfully processed %s: snapshot_id=%d, version=%d, duration=%dms",
                filename, snapshot.snapshot_id, snapshot.version_number, duration_ms,
            )
            return True
            
        except Exception as e:
            session.rollback()
            
            if attempt < max_retries:
                wait_time = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                logger.warning(
                    "Retry %d/%d for %s after error: %s (waiting %ds)",
                    attempt, max_retries, filename, e, wait_time,
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    "Failed to process %s after %d attempts: %s",
                    filename, max_retries, e,
                )
                
                # Create failed upload record for audit
                try:
                    file_hash = compute_file_hash(file_path)
                    upload = create_upload_record(
                        session,
                        filename=filename,
                        file_path=str(file_path.absolute()),
                        file_hash=file_hash,
                        file_size_bytes=file_path.stat().st_size,
                    )
                    mark_upload_failed(
                        session, upload, str(e),
                        duration_ms=int((time.time() - file_start) * 1000),
                    )
                    session.commit()
                except Exception:
                    session.rollback()
                
                pipeline_result.files_failed += 1
                pipeline_result.errors.append({
                    "filename": filename,
                    "error": str(e),
                    "attempts": max_retries,
                })
                pipeline_result.file_details.append({
                    "filename": filename,
                    "status": "failed",
                    "reason": str(e),
                    "attempts": max_retries,
                })
                return False
    
    return False


def run_pipeline(
    data_dir: str | Path | None = None,
    max_retries: int = 3,
) -> PipelineResult:
    """
    Execute the full ETL pipeline.
    
    Discovers .xlsm files, processes each through Extract → Validate → Transform → Load,
    tracks pipeline state, and generates a comprehensive execution report.
    
    Args:
        data_dir: Directory containing .xlsm files (defaults to config)
        max_retries: Max retry attempts per file (exponential backoff)
        
    Returns:
        PipelineResult with execution details
    """
    settings = get_settings()
    data_dir = Path(data_dir) if data_dir else settings.data_path
    
    run_id = str(uuid.uuid4())
    result = PipelineResult(run_id)
    
    logger.info("=" * 60)
    logger.info("PIPELINE STARTED: run_id=%s", run_id)
    logger.info("Data directory: %s", data_dir)
    logger.info("=" * 60)
    
    # Ensure database is initialized
    init_database()
    
    # Discover files
    xlsm_files = sorted(data_dir.glob("*.xlsm"))
    result.files_found = len(xlsm_files)
    
    if not xlsm_files:
        logger.warning("No .xlsm files found in %s", data_dir)
        result.status = "completed"
        result.completed_at = datetime.utcnow()
        return result
    
    logger.info("Found %d .xlsm files to process", len(xlsm_files))
    
    # Create pipeline run record
    session_factory = get_session_factory()
    session = session_factory()
    
    try:
        pipeline_run = PipelineRun(
            run_id=run_id,
            started_at=result.started_at,
            status="running",
            files_found=result.files_found,
        )
        session.add(pipeline_run)
        session.commit()
    except Exception as e:
        logger.error("Failed to create pipeline run record: %s", e)
        session.rollback()
    
    # Process each file
    for file_path in xlsm_files:
        logger.info("-" * 40)
        logger.info("Processing: %s", file_path.name)
        _process_single_file(session, file_path, run_id, result, max_retries)
    
    # Finalize pipeline run
    result.completed_at = datetime.utcnow()
    elapsed_ms = int((result.completed_at - result.started_at).total_seconds() * 1000)
    
    if result.files_failed > 0 and result.files_processed > 0:
        result.status = "partial"
    elif result.files_failed > 0:
        result.status = "failed"
    else:
        result.status = "completed"
    
    try:
        pipeline_run.completed_at = result.completed_at
        pipeline_run.status = result.status
        pipeline_run.files_processed = result.files_processed
        pipeline_run.files_skipped = result.files_skipped
        pipeline_run.files_failed = result.files_failed
        pipeline_run.records_inserted = result.records_inserted
        pipeline_run.execution_time_ms = elapsed_ms
        pipeline_run.quality_summary = result.to_dict().get("quality_summary")
        pipeline_run.error_details = result.errors if result.errors else None
        session.commit()
    except Exception as e:
        logger.error("Failed to update pipeline run record: %s", e)
        session.rollback()
    finally:
        session.close()
    
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETED: run_id=%s", run_id)
    logger.info(
        "Status: %s | Processed: %d | Skipped: %d | Failed: %d | Duration: %dms",
        result.status,
        result.files_processed,
        result.files_skipped,
        result.files_failed,
        elapsed_ms,
    )
    logger.info("=" * 60)
    
    return result
