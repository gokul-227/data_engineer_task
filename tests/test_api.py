"""
API integration tests using FastAPI TestClient.

Tests API endpoints without requiring a running server or database.
Uses httpx for async-compatible test client.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from fastapi.testclient import TestClient


# ============================================================================
# Mock the database and pipeline before importing the app
# ============================================================================

# We need to mock the lifespan to prevent DB init and pipeline run during tests
@pytest.fixture
def client():
    """Create a test client with mocked database."""
    with patch("src.api.main.init_database"), \
         patch("src.api.main.run_pipeline"), \
         patch("src.models.database.get_engine") as mock_engine:
        
        # Import after patching
        from src.api.main import app
        
        with TestClient(app) as c:
            yield c


# ============================================================================
# ROOT & HEALTH TESTS
# ============================================================================

class TestRootEndpoints:
    """Tests for root and health endpoints."""

    def test_root(self, client):
        """Root endpoint should return API info."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert data["documentation"] == "/docs"

    def test_health(self, client):
        """Health endpoint should return status."""
        with patch("src.api.main.check_database_health") as mock_health:
            mock_health.return_value = {"status": "healthy", "database": "connected"}
            response = client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"

    def test_openapi_docs(self, client):
        """OpenAPI JSON should be accessible."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "paths" in data
        assert "info" in data


# ============================================================================
# COMPANY ENDPOINT STRUCTURE TESTS
# ============================================================================

class TestCompanyEndpoints:
    """Tests for company endpoint structure and routing."""

    def test_list_companies_route_exists(self, client):
        """Companies list endpoint should exist (may return empty with mocked DB)."""
        with patch("src.api.routes.companies.get_db"):
            # Just verify the route is registered
            response = client.get("/api/v1/companies")
            # Will fail with DB error, but route should exist (not 404)
            assert response.status_code != 404

    def test_compare_requires_company_ids(self, client):
        """Compare endpoint should require company_ids parameter."""
        response = client.get("/api/v1/companies/compare")
        assert response.status_code == 422  # Validation error

    def test_company_versions_route(self, client):
        """Versions endpoint should exist."""
        response = client.get("/api/v1/companies/test_company/versions")
        assert response.status_code != 404

    def test_company_history_route(self, client):
        """History endpoint should exist."""
        response = client.get("/api/v1/companies/test_company/history")
        assert response.status_code != 404


# ============================================================================
# SNAPSHOT ENDPOINT STRUCTURE TESTS
# ============================================================================

class TestSnapshotEndpoints:
    """Tests for snapshot endpoint structure."""

    def test_snapshots_list_route(self, client):
        response = client.get("/api/v1/snapshots")
        assert response.status_code != 404

    def test_snapshots_latest_route(self, client):
        response = client.get("/api/v1/snapshots/latest")
        assert response.status_code != 404


# ============================================================================
# UPLOAD ENDPOINT STRUCTURE TESTS
# ============================================================================

class TestUploadEndpoints:
    """Tests for upload endpoint structure."""

    def test_uploads_list_route(self, client):
        response = client.get("/api/v1/uploads")
        assert response.status_code != 404

    def test_uploads_stats_route(self, client):
        response = client.get("/api/v1/uploads/stats")
        assert response.status_code != 404
