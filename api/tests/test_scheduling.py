"""Smoke tests for scheduling endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("AIRTABLE_API_KEY", "test_key")
    monkeypatch.setenv("AIRTABLE_BASE_ID", "test_base")
    monkeypatch.setenv("AIRTABLE_TABLE_NAME", "experiment_queue")


def _make_mock_api():
    """Build a mock pyairtable.Api that returns empty data."""
    mock_api = MagicMock()
    mock_table = MagicMock()
    mock_table.all.return_value = []
    mock_table.get.return_value = {"id": "rec1", "fields": {}}
    mock_table.batch_create.return_value = []
    mock_table.batch_update.return_value = []
    mock_table.batch_delete.return_value = []
    mock_api.table.return_value = mock_table
    return mock_api


class TestSchedulingPreview:
    """Tests for GET /api/scheduling/preview."""

    def test_preview_returns_structured_response(
        self,
        client,
        mock_env,
    ):
        """Verify the preview endpoint returns the expected shape."""
        with patch("pyairtable.Api", return_value=_make_mock_api()):
            resp = client.get("/api/scheduling/preview")

        assert resp.status_code == 200
        body = resp.json()
        assert "scheduled_experiments" in body
        assert "in_progress_experiments" in body
        assert "already_scheduled_experiments" in body
        assert "deferred_experiments" in body
        assert "total_cages" in body
        assert "total_boxes" in body
        assert isinstance(body["scheduled_experiments"], list)

    def test_preview_empty_queue(self, client, mock_env):
        """Empty queue returns empty experiment lists."""
        with patch("pyairtable.Api", return_value=_make_mock_api()):
            resp = client.get("/api/scheduling/preview")

        assert resp.status_code == 200
        body = resp.json()
        assert body["scheduled_experiments"] == []
        assert body["total_cages"] == 0
        assert body["total_boxes"] == 0

    def test_preview_invalid_date_returns_400(
        self,
        client,
        mock_env,
    ):
        """Invalid start_date query param returns 400."""
        resp = client.get("/api/scheduling/preview?start_date=not-a-date")
        assert resp.status_code == 400


class TestSchedulingPush:
    """Tests for POST /api/scheduling/push."""

    def test_push_with_empty_list(self, client, mock_env):
        """Pushing an empty list returns success."""
        with patch("pyairtable.Api", return_value=_make_mock_api()):
            resp = client.post(
                "/api/scheduling/push",
                json={"scheduled_experiments": []},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True

    def test_push_creates_records(self, client, mock_env):
        """Pushing experiments calls batch_create."""
        mock_api = _make_mock_api()
        mock_table = mock_api.table.return_value
        mock_table.batch_create.return_value = [{"id": "rec_new"}]
        mock_table.all.return_value = [
            {"id": "m1", "fields": {"manipulation": "m0000001"}},
        ]

        with patch(
            "app.services.scheduling_orchestrator.Api",
            return_value=mock_api,
        ):
            resp = client.post(
                "/api/scheduling/push",
                json={
                    "scheduled_experiments": [
                        {
                            "record_id": "rec1",
                            "experiment_id": "exp1",
                            "assignment": "pseudorandom",
                            "priority": 1,
                            "num_days": 2,
                            "scheduled_start_date": "2026-04-13",
                            "scheduled_end_date": "2026-04-15",
                            "experiment_time_daily": 60.0,
                            "experiment_time_total": 120.0,
                            "assigned_cages": ["c0001"],
                            "assigned_cage_record_ids": [],
                            "cage_to_manip_map": {},
                            "syringe_colors": {},
                            "manipulation_ids": [],
                            "notes": "",
                            "config_file": "default.json",
                            "cages_per_manip": 4,
                            "warnings": [],
                            "status": "scheduled",
                            "deferral_reason": None,
                            "tasks": [],
                        }
                    ]
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True


class TestSchedulingClear:
    """Tests for POST /api/scheduling/clear."""

    def test_clear_returns_success(self, client, mock_env):
        """Clear endpoint returns success with counts."""
        with patch(
            "app.services.scheduling_orchestrator.Api",
            return_value=_make_mock_api(),
        ):
            resp = client.post("/api/scheduling/clear")

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "deleted_count" in body
        assert "cleared_count" in body


class TestSchedulingRecalculate:
    """Tests for POST /api/scheduling/recalculate."""

    def test_recalculate_returns_success(self, client, mock_env):
        """Recalculate returns success with update count."""
        mock_api = _make_mock_api()
        with (
            patch(
                "app.services.scheduling_orchestrator.Api",
                return_value=mock_api,
            ),
            patch(
                "app.helpers.airtable_helpers.Api",
                return_value=mock_api,
            ),
            patch(
                "app.services.scheduling_orchestrator.get_task_times_dict",
                return_value={"task1": {"time": 5}},
            ),
            patch(
                "app.services.scheduling_orchestrator.get_all_experiments_from_queue",
                return_value=[],
            ),
        ):
            resp = client.post("/api/scheduling/recalculate")

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "updated_count" in body
