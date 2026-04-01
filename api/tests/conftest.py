"""Test fixtures for FastAPI application."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def mock_airtable_api():
    """Mock pyairtable.Api to prevent real API calls."""
    with patch("pyairtable.Api") as mock_api_cls:
        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api

        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_table.get.return_value = {"id": "rec123", "fields": {}}
        mock_table.create.return_value = {"id": "rec123", "fields": {}}
        mock_table.update.return_value = {"id": "rec123", "fields": {}}
        mock_table.delete.return_value = True
        mock_table.batch_create.return_value = []

        mock_api.table.return_value = mock_table

        yield mock_api


@pytest.fixture
def mock_env(monkeypatch):
    """Set required environment variables for tests."""
    monkeypatch.setenv("AIRTABLE_API_KEY", "test_key")
    monkeypatch.setenv("AIRTABLE_BASE_ID", "test_base")
    monkeypatch.setenv("AIRTABLE_TABLE_NAME", "experiment_queue")
