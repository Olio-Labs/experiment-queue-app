"""Pydantic request/response schemas for the scheduling API."""

from __future__ import annotations

from pydantic import BaseModel


class ScheduledExperimentResult(BaseModel):
    """A single experiment's scheduling result."""

    record_id: str
    experiment_id: str | None = None
    assignment: str
    priority: int | str = 0
    num_days: int = 1
    scheduled_start_date: str = ""
    scheduled_end_date: str = ""
    experiment_time_daily: float = 0.0
    experiment_time_total: float = 0.0
    assigned_cages: list[str] = []
    assigned_cage_record_ids: list[str] = []
    cage_to_manip_map: dict[str, list[str]] = {}
    syringe_colors: dict[str, str] = {}
    manipulation_ids: list[str] = []
    notes: str = ""
    config_file: str = "default_config.json"
    cages_per_manip: int | str | None = None
    warnings: list[str] = []
    status: str | None = None
    deferral_reason: str | None = None
    tasks: list[str] = []


class SchedulingPreviewResponse(BaseModel):
    """Full scheduling preview response."""

    scheduled_experiments: list[ScheduledExperimentResult] = []
    in_progress_experiments: list[ScheduledExperimentResult] = []
    already_scheduled_experiments: list[ScheduledExperimentResult] = []
    deferred_experiments: list[ScheduledExperimentResult] = []
    cage_heatmap_data: dict = {}
    cage_usage_chart_data: list[dict] = []
    washout_violations: list[str] = []
    drug_warnings: list[str] = []
    available_syringe_colors: list[str] = []
    total_cages: int = 0
    total_boxes: int = 0
    scheduling_errors: list[str] = []


class PushPlanRequest(BaseModel):
    """Request body for pushing the plan to Airtable."""

    scheduled_experiments: list[ScheduledExperimentResult]


class PushPlanResponse(BaseModel):
    """Response from pushing the plan."""

    success: bool
    message: str = ""
    updated_count: int = 0
    created_count: int = 0
    planner_records_created: int = 0
    errors: list[str] = []
