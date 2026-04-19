"""
SQLAlchemy database engine and session management.

Provides connection pooling, session factory, and initialization utilities.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings

logger = logging.getLogger(__name__)

# Global engine and session factory — initialized lazily
_engine = None
_SessionFactory = None


def get_engine():
    """Get or create the SQLAlchemy engine (singleton)."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=settings.api_debug,
        )
        logger.info("Database engine created: %s", settings.postgres_host)
    return _engine


def get_session_factory() -> sessionmaker:
    """Get or create the session factory (singleton)."""
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _SessionFactory


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency — yields a database session and ensures cleanup.
    
    Usage:
        @app.get("/endpoint")
        def handler(db: Session = Depends(get_db)):
            ...
    """
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def init_database() -> None:
    """
    Initialize the database schema from the SQL file.
    
    Called on first startup to create tables if they don't exist.
    Idempotent — safe to run multiple times.
    """
    engine = get_engine()
    sql_path = Path(__file__).parent.parent / "db" / "init_schema.sql"

    if not sql_path.exists():
        logger.error("Schema file not found: %s", sql_path)
        raise FileNotFoundError(f"Schema file not found: {sql_path}")

    logger.info("Initializing database schema from %s", sql_path)
    sql_content = sql_path.read_text()

    # Use raw connection to execute the entire Postgres script at once
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute(sql_content)
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        logger.error("Database schema initialization failed: %s", e)
        raise
    finally:
        raw_conn.close()

    logger.info("Database schema initialization complete")


def check_database_health() -> dict:
    """Check database connectivity and return health status."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error("Database health check failed: %s", e)
        return {"status": "unhealthy", "database": str(e)}
