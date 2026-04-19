"""
FastAPI application entry point.

Configures the application with:
- All API route registrations
- CORS middleware
- Health check endpoint
- Startup event for DB initialization and pipeline execution
- OpenAPI documentation
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import companies, snapshots, uploads
from src.models.database import check_database_health, init_database
from src.models.pydantic_models import HealthCheck
from src.etl.pipeline import run_pipeline
from src.config import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    On startup:
    1. Initialize database schema
    2. Run ETL pipeline to load any new data files
    """
    logger.info("Application starting up...")
    
    # Initialize database
    try:
        init_database()
        logger.info("Database schema initialized")
    except Exception as e:
        logger.error("Failed to initialize database: %s", e)
        raise
    
    # Run ETL pipeline to load data
    try:
        settings = get_settings()
        logger.info("Running ETL pipeline on startup...")
        result = run_pipeline(data_dir=settings.data_dir)
        logger.info(
            "Pipeline completed: status=%s, processed=%d, skipped=%d, failed=%d",
            result.status,
            result.files_processed,
            result.files_skipped,
            result.files_failed,
        )
    except Exception as e:
        logger.error("Pipeline execution failed: %s", e)
        # Don't prevent startup — API should still be available
    
    yield
    
    logger.info("Application shutting down...")


# ============================================================================
# APPLICATION SETUP
# ============================================================================

app = FastAPI(
    title="Corporate Credit Rating Data Pipeline API",
    description=(
        "RESTful API for corporate credit rating analytics. Provides access to "
        "company metadata, rating snapshots, version history, time-series analysis, "
        "and point-in-time comparisons extracted from Excel-based rating assessments.\n\n"
        "### Key Features\n"
        "- **Company Management**: List, detail, and compare companies\n"
        "- **Version Control**: Track changes across multiple uploads per company\n"
        "- **Time-Series Analysis**: Historical evolution of ratings and risk profiles\n"
        "- **Point-in-Time Queries**: Compare companies at any historical date\n"
        "- **Data Quality**: Quality scores and validation reports per file\n"
        "- **Audit Trail**: Complete file upload history with download capability"
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# REGISTER ROUTES
# ============================================================================

app.include_router(companies.router, prefix="/api/v1")
app.include_router(snapshots.router, prefix="/api/v1")
app.include_router(uploads.router, prefix="/api/v1")


# ============================================================================
# ROOT & HEALTH ENDPOINTS
# ============================================================================

@app.get("/", tags=["Root"])
def root():
    """API root — welcome message and documentation links."""
    return {
        "name": "Corporate Credit Rating Data Pipeline API",
        "version": "1.0.0",
        "documentation": "/docs",
        "redoc": "/redoc",
        "health": "/health",
        "api_base": "/api/v1",
    }


@app.get("/health", response_model=HealthCheck, tags=["Health"])
def health_check():
    """Health check endpoint for service monitoring."""
    db_health = check_database_health()
    return HealthCheck(
        status="healthy" if db_health["status"] == "healthy" else "degraded",
        database=db_health["database"],
        api_version="1.0.0",
        timestamp=datetime.utcnow(),
    )
