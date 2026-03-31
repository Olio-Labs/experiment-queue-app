"""Tests for FastAPI endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_health_check(client):
    """Health endpoint returns ok."""
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_experiments_returns_200(client, mock_env, mock_airtable_api):
    """Experiments endpoint returns 200 with mocked Airtable."""
    with patch("app.routers.experiments.AirtableBase") as mock_base_cls:
        mock_base = MagicMock()
        mock_base_cls.from_env.return_value = mock_base

        with patch(
            "app.routers.experiments.ExperimentsRepository"
        ) as mock_repo_cls:
            mock_repo = MagicMock()
            mock_repo.list_active.return_value = []
            mock_repo_cls.return_value = mock_repo

            with patch(
                "app.routers.experiments.ManipulationsRepository"
            ) as mock_manip_cls:
                mock_manip = MagicMock()
                mock_manip.list_all_manipulations.return_value = []
                mock_manip_cls.return_value = mock_manip

                response = client.get("/api/experiments")
                assert response.status_code == 200
                data = response.json()
                assert "experiments" in data
                assert "headers" in data


def test_get_form_options(client, mock_env):
    """Form options endpoint returns 200."""
    with patch(
        "app.routers.experiments.get_all_dropdown_options"
    ) as mock_opts:
        mock_opts.return_value = {"priority": ["1", "2", "3"]}
        response = client.get("/api/experiments/form-options")
        assert response.status_code == 200
        assert "options" in response.json()


def test_delete_experiment(client, mock_env, mock_airtable_api):
    """Delete endpoint returns success."""
    response = client.delete("/api/experiments/rec123")
    assert response.status_code == 200
    assert response.json()["success"] is True


def test_list_cages(client, mock_env):
    """Cages endpoint returns stats."""
    with patch("app.routers.cages.get_all_cages") as mock_cages:
        mock_cages.return_value = [
            {"id": "rec1", "fields": {"sex": "m"}},
            {"id": "rec2", "fields": {"sex": "f"}},
        ]
        response = client.get("/api/cages")
        assert response.status_code == 200
        stats = response.json()["cage_stats"]
        assert stats["total"] == 2
        assert stats["male"] == 1
        assert stats["female"] == 1


def test_weekly_calendar(client, mock_env):
    """Calendar endpoint returns URL."""
    response = client.get("/api/calendar/weekly")
    assert response.status_code == 200
    assert "calendar_url" in response.json()


def test_scheduling_preview(client, mock_env):
    """Scheduling preview returns data."""
    with patch(
        "app.routers.scheduling.get_all_experiments_from_queue"
    ) as mock_exps:
        mock_exps.return_value = []
        with patch("app.routers.scheduling.get_all_cages") as mock_cages:
            mock_cages.return_value = []
            with patch("app.routers.scheduling.get_all_boxes") as mock_boxes:
                mock_boxes.return_value = []
                with patch(
                    "app.routers.scheduling.get_all_manipulations"
                ) as mock_manips:
                    mock_manips.return_value = []
                    response = client.get("/api/scheduling/preview")
                    assert response.status_code == 200
