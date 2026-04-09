"""Scheduling orchestrator — coordinates the full scheduling workflow.

Extracts logic from the original app.py experiment_plan_preview(),
push_plan_to_airtable_route(), clear_scheduled_plan_route(), and
recalculate_all_experiment_times_route().
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from pyairtable import Api

from ..config import Settings, settings
from ..domain.experiment import ExperimentContext, ExperimentFactory
from ..helpers.airtable_helpers import (
    extract_options_from_field_schema,
    get_all_boxes,
    get_all_cages,
    get_all_drugs_details,
    get_all_experiments_from_queue,
    get_all_manipulations_details,
    get_all_records,
    get_existing_syringe_color_assignments_from_planner,
    get_experiment_planner_history_from_airtable,
    get_in_progress_experiments_from_queue,
    get_potential_cage_pool_from_airtable,
    get_scheduled_experiments_from_queue,
    get_table_schema_from_metadata,
    get_task_times_dict,
)
from ..helpers.scheduling_helpers import (
    DAYS_OF_WEEK_ORDERED,
    TEMP_TECHNICIAN_AVAILABILITY,
    calculate_total_mice_for_experiment,
    check_and_assign_cages_for_period,
    check_and_reserve_syringe_colors_for_experiment_duration,
    check_drug_availability_for_period,
    check_technician_resources_for_period,
    create_cage_to_box_mapping,
    extract_washout_violations_from_notes,
    get_technicians_and_capacity_per_day,
    sort_experiments_for_scheduling,
)
from ..schemas import (
    PushPlanRequest,
    PushPlanResponse,
    ScheduledExperimentResult,
    SchedulingPreviewResponse,
)
from ..services.date_range import DateRange
from ..services.notes_parser import parse_notes
from ..services.scheduling_service import (
    fetch_technician_availability_date_range,
    precommit_in_progress_resources_date_range,
    precommit_scheduled_resources_date_range,
    prepare_cage_heatmap_data,
)

logger = logging.getLogger(__name__)


class SchedulingOrchestrator:
    """Coordinates experiment scheduling: preview, push, clear, recalculate."""

    def __init__(self, cfg: Settings | None = None) -> None:
        self.cfg = cfg or settings
        self._api_key = self.cfg.airtable_api_key
        self._base_id = self.cfg.airtable_base_id

    # ------------------------------------------------------------------
    # Preview
    # ------------------------------------------------------------------

    def compute_preview(
        self, start_date: date | None = None
    ) -> SchedulingPreviewResponse:
        """Compute a full scheduling preview."""
        # ---- 1. Fetch syringe color options ----
        syringe_colors_master = self._fetch_syringe_color_options()
        if not syringe_colors_master:
            return SchedulingPreviewResponse(
                scheduling_errors=["No valid syringe color options found."],
            )

        # ---- 2. Fetch all data (parallelized) ----
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=8) as pool:
            f_experiments = pool.submit(
                get_all_experiments_from_queue,
                self._api_key,
                self._base_id,
                self.cfg.airtable_table_name,
            )
            f_cages = pool.submit(
                get_all_cages,
                self._api_key,
                self._base_id,
            )
            f_boxes = pool.submit(
                get_all_boxes,
                self._api_key,
                self._base_id,
            )
            f_manips = pool.submit(
                get_all_manipulations_details,
                self._api_key,
                self._base_id,
            )
            f_drugs = pool.submit(
                get_all_drugs_details,
                self._api_key,
                self._base_id,
            )
            f_inventory = pool.submit(
                get_all_records,
                self._api_key,
                self._base_id,
                "drug_inventory",
            )
            f_task_times = pool.submit(
                get_task_times_dict,
                self._api_key,
                self._base_id,
            )
            f_cage_pool = pool.submit(
                get_potential_cage_pool_from_airtable,
                self._api_key,
                self._base_id,
            )
            f_syringe = pool.submit(
                get_existing_syringe_color_assignments_from_planner,
                self._api_key,
                self._base_id,
            )
            f_in_progress = pool.submit(
                get_in_progress_experiments_from_queue,
                self._api_key,
                self._base_id,
                self.cfg.airtable_table_name,
            )
            f_scheduled = pool.submit(
                get_scheduled_experiments_from_queue,
                self._api_key,
                self._base_id,
                self.cfg.airtable_table_name,
            )
            f_manip_records = pool.submit(
                get_all_records,
                self._api_key,
                self._base_id,
                "manipulations",
            )

        experiments = f_experiments.result()
        all_cages = f_cages.result()
        all_boxes = f_boxes.result()
        all_manips_map = f_manips.result()
        all_drugs_map = f_drugs.result()
        all_drug_inventory = f_inventory.result()
        task_times = f_task_times.result()
        cage_pool = f_cage_pool.result()
        existing_syringe = f_syringe.result()
        in_progress_exps = f_in_progress.result()
        scheduled_exps = f_scheduled.result()
        all_manip_records = f_manip_records.result()

        # Second batch: planner_history depends on cage_pool
        relevant_ids = [c["custom_cage_id"] for c in cage_pool] if cage_pool else []
        planner_history = get_experiment_planner_history_from_airtable(
            self._api_key,
            self._base_id,
            relevant_ids,
        )
        manip_rid_to_custom: dict[str, str] = {
            r["id"]: r["fields"].get("manipulation", "")
            for r in all_manip_records
            if "id" in r and "fields" in r and "manipulation" in r["fields"]
        }
        manip_name_to_rid: dict[str, str] = {
            v: k for k, v in manip_rid_to_custom.items()
        }
        box_rid_to_id: dict[str, str] = {
            b["id"]: b["fields"].get("box_id", "unknown")
            for b in all_boxes
            if "id" in b and "fields" in b
        }
        cage_to_box_map = create_cage_to_box_mapping(
            all_cages,
            all_boxes,
        )

        # ---- 3. Scheduling window ----
        today = datetime.now().date()
        if start_date:
            sched_start = start_date
        else:
            mon = today - timedelta(days=today.weekday())
            sched_start = mon + timedelta(days=7)

        preview_days = 7
        week_dates = [sched_start + timedelta(days=i) for i in range(preview_days)]
        preview_range = DateRange(week_dates[0], week_dates[-1])

        # ---- 4. State accumulators (weekday-keyed for tech check) ----
        daily_time_booked: dict[str, float] = defaultdict(float)
        daily_mice_booked: dict[str, int] = defaultdict(int)
        daily_cages_booked: dict[str, int] = defaultdict(int)
        daily_boxes_booked: dict[str, set] = defaultdict(set)
        daily_nonbox_booked: dict[str, int] = defaultdict(int)
        preview_booked_cages: dict[str, set[date]] = defaultdict(set)

        # ---- 5. Tech availability ----
        tech_avail = dict(TEMP_TECHNICIAN_AVAILABILITY)
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            cal_id = self.cfg.google_tech_calendar_id
            sa_json = self.cfg.google_service_account_json
            sa_file = self.cfg.google_service_account_file
            if cal_id and (sa_json or sa_file):
                scopes = ["https://www.googleapis.com/auth/calendar"]
                if sa_json:
                    import json as _json

                    info = _json.loads(sa_json)
                    creds = service_account.Credentials.from_service_account_info(
                        info, scopes=scopes
                    )
                else:
                    creds = service_account.Credentials.from_service_account_file(
                        sa_file, scopes=scopes
                    )
                service = build(
                    "calendar",
                    "v3",
                    credentials=creds,
                )
                avail_by_date, err = fetch_technician_availability_date_range(
                    service,
                    cal_id,
                    preview_range,
                    self.cfg.calendar_timezone,
                    valid_names=[
                        "Henry",
                        "Angie",
                        "James",
                        "Kevin",
                        "Tom",
                        "David",
                        "Tim",
                        "Gina",
                    ],
                    default_hours=4,
                )
                if not err and avail_by_date:
                    tech_avail = {d: [] for d in DAYS_OF_WEEK_ORDERED}
                    for d, entries in avail_by_date.items():
                        dn = DAYS_OF_WEEK_ORDERED[d.weekday()]
                        tech_avail[dn].extend(entries)
        except Exception as e:
            logger.info(f"Calendar fallback: {e}")

        daily_tech_details = get_technicians_and_capacity_per_day(
            tech_avail,
        )

        # ---- 6. Pre-book daily overhead ----
        if task_times:
            overhead = sum(
                task_times.get(t, 0)
                for t in [
                    "mop_box_room",
                    "mop_cage_room",
                    "experiment_setup",
                    "experiment_cleanup",
                ]
            )
            if overhead > 0:
                for dn in DAYS_OF_WEEK_ORDERED:
                    daily_time_booked[dn] += overhead

        # ---- 7. Syringe color state ----
        daily_used_colors: dict[date, set[str]] = defaultdict(set)
        daily_color_owners: dict[date, dict[str, str]] = defaultdict(dict)
        manip_persistent_color: dict[str, str] = {}

        for dk, colors in existing_syringe.items():
            daily_used_colors[dk].update(colors)
            for c in colors:
                daily_color_owners[dk].setdefault(c, "UNKNOWN")

        # ---- 8. Effective last use tracker ----
        eff_last_use: dict[str, date] = {}
        for h in planner_history:
            cid = h["cage_id"]
            mid = h["manipulation_id"]
            idate = h["start_date"]
            if mid in ("m0000000", "m0000004"):
                continue
            if cid not in eff_last_use or idate > eff_last_use[cid]:
                eff_last_use[cid] = idate

        # ---- 9. Pre-commit in-progress ----
        in_progress_ids = {e.get("id") for e in in_progress_exps if e.get("id")}
        experiments = [e for e in experiments if "fields" in e]
        exps_for_sched = [e for e in experiments if e.get("id") not in in_progress_ids]

        try:
            dk_time = {d: 0.0 for d in preview_range.iter_days()}
            dk_mice = {d: 0 for d in preview_range.iter_days()}
            dk_cages = {d: 0 for d in preview_range.iter_days()}
            dk_boxes: dict[date, set] = {d: set() for d in preview_range.iter_days()}
            dk_nonbox = {d: 0 for d in preview_range.iter_days()}

            in_prog_processed = precommit_in_progress_resources_date_range(
                in_progress_exps,
                preview_range,
                dk_time,
                dk_mice,
                preview_booked_cages,
                all_cages,
                dk_cages,
                dk_boxes,
                dk_nonbox,
                box_rid_to_id,
            )

            for d in preview_range.iter_days():
                dn = DAYS_OF_WEEK_ORDERED[d.weekday()]
                daily_time_booked[dn] += dk_time.get(d, 0.0)
                daily_mice_booked[dn] += dk_mice.get(d, 0)
                daily_cages_booked[dn] += dk_cages.get(d, 0)
                daily_boxes_booked[dn].update(dk_boxes.get(d, set()))
                daily_nonbox_booked[dn] += dk_nonbox.get(d, 0)
        except Exception as e:
            logger.info(f"In-progress precommit error: {e}")
            in_prog_processed = []

        # ---- 10. Pre-commit scheduled ----
        sched_ids = {e.get("id") for e in scheduled_exps if e.get("id")}
        exps_for_sched = [e for e in exps_for_sched if e.get("id") not in sched_ids]

        try:
            dk_time_s = {d: 0.0 for d in preview_range.iter_days()}
            dk_mice_s = {d: 0 for d in preview_range.iter_days()}
            dk_cages_s = {d: 0 for d in preview_range.iter_days()}
            dk_boxes_s: dict[date, set] = {d: set() for d in preview_range.iter_days()}
            dk_nonbox_s = {d: 0 for d in preview_range.iter_days()}

            sched_processed = precommit_scheduled_resources_date_range(
                scheduled_exps,
                preview_range,
                dk_time_s,
                dk_mice_s,
                preview_booked_cages,
                all_cages,
                dk_cages_s,
                dk_boxes_s,
                dk_nonbox_s,
                box_rid_to_id,
                manip_rid_to_custom,
            )

            for d in preview_range.iter_days():
                dn = DAYS_OF_WEEK_ORDERED[d.weekday()]
                daily_time_booked[dn] += dk_time_s.get(d, 0.0)
                daily_mice_booked[dn] += dk_mice_s.get(d, 0)
                daily_cages_booked[dn] += dk_cages_s.get(d, 0)
                daily_boxes_booked[dn].update(dk_boxes_s.get(d, set()))
                daily_nonbox_booked[dn] += dk_nonbox_s.get(d, 0)
        except Exception as e:
            logger.info(f"Scheduled precommit error: {e}")
            sched_processed = []

        # ---- 11. Main scheduling loop ----
        sorted_exps = sort_experiments_for_scheduling(exps_for_sched)
        unscheduled = list(sorted_exps)
        scheduled_output: list[ScheduledExperimentResult] = []
        deferred_output: list[ScheduledExperimentResult] = []
        conflicts: list[str] = []
        all_washout_violations: list[str] = []

        for current_date in week_dates:
            day_name = DAYS_OF_WEEK_ORDERED[current_date.weekday()]
            retry_next_day: list[dict] = []

            for exp_rec in unscheduled:
                result = self._try_schedule_experiment(
                    exp_rec,
                    current_date,
                    day_name,
                    all_cages,
                    all_boxes,
                    cage_pool,
                    planner_history,
                    all_manips_map,
                    all_drugs_map,
                    all_drug_inventory,
                    task_times,
                    manip_rid_to_custom,
                    manip_name_to_rid,
                    preview_booked_cages,
                    daily_time_booked,
                    daily_mice_booked,
                    daily_cages_booked,
                    daily_boxes_booked,
                    daily_nonbox_booked,
                    daily_tech_details,
                    tech_avail,
                    daily_used_colors,
                    daily_color_owners,
                    manip_persistent_color,
                    syringe_colors_master,
                    eff_last_use,
                    cage_to_box_map,
                    box_rid_to_id,
                    week_dates,
                )

                if result is None:
                    retry_next_day.append(exp_rec)
                elif isinstance(result, ScheduledExperimentResult):
                    scheduled_output.append(result)
                    if result.warnings:
                        conflicts.extend(result.warnings)

            unscheduled = retry_next_day

        # Remaining unscheduled → deferred
        for exp_rec in unscheduled:
            f = exp_rec.get("fields", {})
            deferred_output.append(
                ScheduledExperimentResult(
                    record_id=exp_rec.get("id", ""),
                    experiment_id=f.get("experiment_id"),
                    assignment=f.get(self.cfg.assignment_field_name, ""),
                    priority=f.get("priority", 0),
                    num_days=int(f.get("num_days", 1)),
                    notes=f.get("notes", ""),
                    status="deferred",
                    deferral_reason="Could not fit in scheduling window",
                )
            )

        # ---- 12. Build heatmap ----
        heatmap = prepare_cage_heatmap_data(
            all_cages,
            preview_booked_cages,
            week_dates,
            [s.model_dump() for s in scheduled_output],
        )

        # Build in-progress results
        in_prog_results = [
            self._make_result_from_processed(p, "in_progress")
            for p in (in_prog_processed or [])
        ]
        sched_results = [
            self._make_result_from_processed(p, "scheduled")
            for p in (sched_processed or [])
        ]

        return SchedulingPreviewResponse(
            scheduled_experiments=scheduled_output,
            in_progress_experiments=in_prog_results,
            already_scheduled_experiments=sched_results,
            deferred_experiments=deferred_output,
            cage_heatmap_data=heatmap,
            washout_violations=all_washout_violations,
            drug_warnings=conflicts,
            available_syringe_colors=syringe_colors_master,
            total_cages=len(all_cages),
            total_boxes=len(all_boxes),
            scheduling_errors=[],
        )

    # ------------------------------------------------------------------
    # Push
    # ------------------------------------------------------------------

    def push_plan(self, request: PushPlanRequest) -> PushPlanResponse:
        """Push scheduled experiments to Airtable."""
        api = Api(self._api_key)
        errors: list[str] = []

        exps_to_push = [
            e
            for e in request.scheduled_experiments
            if not e.status or e.status in ("split", "scheduled")
        ]
        if not exps_to_push:
            return PushPlanResponse(
                success=True,
                message="No new experiments to push.",
            )

        target_table = api.table(
            self._base_id,
            self.cfg.experiments_copy_testing_table_name,
        )
        planner_table = api.table(
            self._base_id,
            self.cfg.experiment_planner_copy_testing_table_name,
        )
        queue_table = api.table(
            self._base_id,
            self.cfg.airtable_table_name,
        )

        # Build lookup maps
        manip_recs = api.table(
            self._base_id,
            "manipulations",
        ).all(fields=["manipulation"])
        manip_name_to_rid = {
            r["fields"]["manipulation"]: r["id"]
            for r in manip_recs
            if "fields" in r and "manipulation" in r["fields"]
        }

        cage_recs = api.table(
            self._base_id,
            "cages",
        ).all(fields=["cage"])
        cage_custom_to_rid = {
            str(r["fields"]["cage"]): r["id"]
            for r in cage_recs
            if "fields" in r and "cage" in r["fields"]
        }

        # Create daily summary records
        daily_records: list[dict] = []
        for exp in exps_to_push:
            if not exp.scheduled_start_date or not exp.scheduled_end_date:
                continue
            try:
                s = datetime.strptime(
                    exp.scheduled_start_date,
                    "%Y-%m-%d",
                ).date()
                e = datetime.strptime(
                    exp.scheduled_end_date,
                    "%Y-%m-%d",
                ).date()
            except ValueError:
                continue

            day = s
            while day < e:
                daily_records.append(
                    {
                        "start_date": day.strftime("%Y-%m-%d"),
                        "end_date": (day + timedelta(days=1)).strftime("%Y-%m-%d"),
                        "duration_hr": 24.0,
                        "block_size_sec": 3600,
                        "config_file": exp.config_file,
                        "notes": exp.notes,
                    }
                )
                day += timedelta(days=1)

        created_count = 0
        if daily_records:
            try:
                created = target_table.batch_create(daily_records)
                created_count = len(created)
            except Exception as e:
                errors.append(f"Daily record creation failed: {e}")

        # Create planner records
        planner_records: list[dict] = []
        for exp in exps_to_push:
            for manip_id, cages in exp.cage_to_manip_map.items():
                manip_rid = manip_name_to_rid.get(manip_id)
                if not manip_rid:
                    continue
                color = exp.syringe_colors.get(manip_id)
                for cage_custom in cages:
                    cage_rid = cage_custom_to_rid.get(str(cage_custom))
                    if not cage_rid:
                        continue
                    entry: dict[str, Any] = {
                        "cage_": [cage_rid],
                        "manipulation_": [manip_rid],
                    }
                    if color:
                        entry["syringe_color"] = [color]
                    planner_records.append(entry)

        planner_count = 0
        if planner_records:
            try:
                planner_table.batch_create(planner_records)
                planner_count = len(planner_records)
            except Exception as e:
                errors.append(f"Planner record creation failed: {e}")

        # Update queue status
        unique_ids = set()
        queue_updates: list[dict] = []
        for exp in exps_to_push:
            if exp.record_id and exp.record_id not in unique_ids:
                unique_ids.add(exp.record_id)
                cage_rids = [cage_custom_to_rid.get(str(c)) for c in exp.assigned_cages]
                cage_rids = [r for r in cage_rids if r]
                fields: dict[str, Any] = {"status": "scheduled"}
                if cage_rids:
                    fields["unique_cage_ids"] = cage_rids
                if exp.scheduled_start_date:
                    fields["actual_start_date"] = exp.scheduled_start_date
                if exp.scheduled_end_date:
                    fields["actual_end_date"] = exp.scheduled_end_date
                if exp.notes:
                    fields["notes"] = exp.notes
                queue_updates.append(
                    {
                        "id": exp.record_id,
                        "fields": fields,
                    }
                )

        if queue_updates:
            try:
                queue_table.batch_update(queue_updates)
            except Exception as e:
                errors.append(f"Queue status update failed: {e}")

        return PushPlanResponse(
            success=len(errors) == 0,
            message=(
                f"{created_count} daily records, "
                f"{planner_count} planner records created."
            ),
            updated_count=len(queue_updates),
            created_count=created_count,
            planner_records_created=planner_count,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Clear
    # ------------------------------------------------------------------

    def clear_plan(self) -> dict:
        """Clear the scheduled plan from Airtable."""
        api = Api(self._api_key)

        today = datetime.now().date()
        days_to_mon = (7 - today.weekday()) % 7
        if days_to_mon == 0:
            days_to_mon = 7
        next_monday = today + timedelta(days=days_to_mon)
        next_sunday = next_monday + timedelta(days=6)

        exp_table = api.table(
            self._base_id,
            self.cfg.experiments_copy_testing_table_name,
        )
        queue_table = api.table(
            self._base_id,
            self.cfg.airtable_table_name,
        )

        # Delete experiment copy records in the target week
        all_recs = exp_table.all(fields=["start_date", "end_date"])
        to_delete: list[str] = []
        for rec in all_recs:
            f = rec.get("fields", {})
            try:
                rs = datetime.strptime(
                    f.get("start_date", ""),
                    "%Y-%m-%d",
                ).date()
                re_ = datetime.strptime(
                    f.get("end_date", rs.strftime("%Y-%m-%d")),
                    "%Y-%m-%d",
                ).date()
                if rs <= next_sunday and re_ >= next_monday:
                    to_delete.append(rec["id"])
            except (ValueError, TypeError):
                continue

        deleted_count = 0
        if to_delete:
            exp_table.batch_delete(to_delete)
            deleted_count = len(to_delete)

        # Clear scheduled experiment statuses
        cleared_count = 0
        try:
            scheduled = queue_table.all(
                formula="{status}='scheduled'",
                fields=["status"],
            )
            if scheduled:
                updates = [
                    {"id": r["id"], "fields": {"status": None}} for r in scheduled
                ]
                queue_table.batch_update(updates)
                cleared_count = len(updates)
        except Exception as e:
            logger.warning(f"Failed to clear queue statuses: {e}")

        return {
            "success": True,
            "message": (
                f"Cleared {deleted_count} daily records and "
                f"{cleared_count} queue statuses for "
                f"{next_monday} to {next_sunday}."
            ),
            "deleted_count": deleted_count,
            "cleared_count": cleared_count,
        }

    # ------------------------------------------------------------------
    # Recalculate
    # ------------------------------------------------------------------

    def recalculate_times(self) -> dict:
        """Recalculate experiment time estimates for all experiments."""
        api = Api(self._api_key)
        table = api.table(
            self._base_id,
            self.cfg.airtable_table_name,
        )

        all_exps = get_all_experiments_from_queue(
            self._api_key,
            self._base_id,
            self.cfg.airtable_table_name,
        )
        task_times = get_task_times_dict(
            self._api_key,
            self._base_id,
        )
        all_cages = get_all_cages(self._api_key, self._base_id)
        all_manips_map = get_all_manipulations_details(
            self._api_key,
            self._base_id,
        )
        all_drugs_map = get_all_drugs_details(
            self._api_key,
            self._base_id,
        )

        if not task_times:
            return {
                "success": False,
                "message": "Could not fetch task times.",
                "updated_count": 0,
                "errors": ["Task times unavailable"],
            }

        updates: list[dict] = []
        errors: list[str] = []

        for rec in all_exps:
            if "fields" not in rec or "id" not in rec:
                continue
            exp_id = rec["id"]
            fields = rec["fields"]
            stored = fields.get("experiment_time")

            ctx = ExperimentContext(
                scheduling_date=None,
                task_times=task_times,
                all_cages=all_cages,
                all_manipulations_map=all_manips_map,
                all_drugs_map=all_drugs_map,
                manip_record_id_to_custom_id={
                    r["id"]: r["fields"].get("manipulation", "")
                    for r in all_manips_map.values()
                    if isinstance(r, dict) and "fields" in r
                },
            )

            try:
                exp_obj = ExperimentFactory.from_airtable_record(rec)
                new_time = exp_obj.estimate_minutes(ctx)
            except Exception as e:
                errors.append(f"{exp_id}: {e}")
                continue

            if new_time is not None:
                if stored is None or abs(float(new_time) - float(stored or 0)) > 0.01:
                    updates.append(
                        {
                            "id": exp_id,
                            "fields": {"experiment_time": new_time},
                        }
                    )

        if updates:
            table.batch_update(updates)

        msg = f"{len(updates)} experiment(s) updated."
        if errors:
            msg += f" {len(errors)} errors."

        return {
            "success": True,
            "message": msg,
            "updated_count": len(updates),
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_syringe_color_options(self) -> list[str]:
        """Fetch and filter syringe color options from planner schema."""
        try:
            schema = get_table_schema_from_metadata(
                self._api_key,
                self._base_id,
                self.cfg.experiment_planner_copy_testing_table_name,
            )
            if not schema:
                return []
            for f_schema in schema.get("fields", []):
                if f_schema.get("name") == "syringe_color":
                    options = extract_options_from_field_schema(
                        f_schema,
                        "syringe_color",
                        self.cfg.experiment_planner_copy_testing_table_name,
                    )
                    if options:
                        return [
                            c
                            for c in options
                            if c is not None
                            and not (isinstance(c, str) and c.lower() == "none")
                            and not (isinstance(c, str) and ("IP" in c or "SC" in c))
                        ]
            return []
        except Exception as e:
            logger.warning(f"Syringe color fetch error: {e}")
            return []

    def _try_schedule_experiment(
        self,
        exp_rec: dict,
        current_date: date,
        day_name: str,
        all_cages: list[dict],
        all_boxes: list[dict],
        cage_pool: list[dict],
        planner_history: list[dict],
        all_manips_map: dict[str, dict],
        all_drugs_map: dict[str, dict],
        all_drug_inventory: list[dict],
        task_times: dict[str, float],
        manip_rid_to_custom: dict[str, str],
        manip_name_to_rid: dict[str, str],
        preview_booked_cages: dict[str, set[date]],
        daily_time_booked: dict[str, float],
        daily_mice_booked: dict[str, int],
        daily_cages_booked: dict[str, int],
        daily_boxes_booked: dict[str, set],
        daily_nonbox_booked: dict[str, int],
        daily_tech_details: dict,
        tech_avail: dict,
        daily_used_colors: dict[date, set[str]],
        daily_color_owners: dict[date, dict[str, str]],
        manip_persistent_color: dict[str, str],
        syringe_colors_master: list[str],
        eff_last_use: dict[str, date],
        cage_to_box_map: dict[str, int],
        box_rid_to_id: dict[str, str],
        week_dates: list[date],
    ) -> ScheduledExperimentResult | None:
        """Try to schedule a single experiment on a given date.

        Returns a ScheduledExperimentResult on success, or None to
        retry on the next day.
        """
        f = exp_rec.get("fields", {})
        exp_id = exp_rec.get("id", "")
        assignment = f.get(self.cfg.assignment_field_name, "").strip().lower()
        if not assignment:
            return None

        num_days = int(f.get("num_days", 1))
        if num_days <= 0:
            num_days = 1
        original_notes = f.get("notes", "")

        # Check earliest start date
        esd_str = f.get("earliest_start_date")
        if esd_str:
            try:
                esd = datetime.strptime(
                    esd_str,
                    "%Y-%m-%d",
                ).date()
                if current_date < esd:
                    return None
            except (ValueError, TypeError):
                pass

        # Determine manip custom IDs for syringe color check
        manip_customs_for_color: set[str] = set()
        exp_manip_rids = f.get("unique_manipulation_ids", [])

        if assignment == "pseudorandom":
            _, manips_list = parse_notes(original_notes)
            for m in manips_list or []:
                if str(m).strip():
                    manip_customs_for_color.add(str(m))
        else:
            for rid in exp_manip_rids:
                cid = manip_rid_to_custom.get(rid)
                if cid:
                    manip_customs_for_color.add(cid)

        manip_customs_for_color.discard("m0000004")

        # Syringe color check
        tentative_colors: dict[str, str] = {}
        if manip_customs_for_color:
            ok, colors, err = check_and_reserve_syringe_colors_for_experiment_duration(
                exp_id,
                manip_customs_for_color,
                current_date,
                num_days,
                daily_used_colors,
                daily_color_owners,
                manip_persistent_color,
                syringe_colors_master,
            )
            if not ok:
                return None
            tentative_colors = colors or {}

        # Time estimation
        exp_time = f.get("experiment_time")
        if exp_time is None:
            try:
                ctx = ExperimentContext(
                    scheduling_date=current_date,
                    task_times=task_times,
                    all_cages=all_cages,
                    all_manipulations_map=all_manips_map,
                    all_drugs_map=all_drugs_map,
                    manip_record_id_to_custom_id=manip_rid_to_custom,
                )
                exp_obj = ExperimentFactory.from_airtable_record(
                    exp_rec,
                )
                exp_time = exp_obj.estimate_minutes(ctx)
            except Exception:
                return None
        if exp_time is None:
            return None
        exp_time_min = float(exp_time)

        # Cage assignment
        assigned_ids, updated_notes, cage_err = check_and_assign_cages_for_period(
            exp_rec,
            current_date,
            all_cages,
            preview_booked_cages,
            cage_pool,
            planner_history,
            manip_rid_to_custom,
            all_manips_map,
            all_drugs_map,
            eff_last_use,
            all_boxes,
        )
        if cage_err:
            return None

        # Washout violations
        final_notes, violations = extract_washout_violations_from_notes(
            updated_notes,
        )

        # Mice count
        num_mice = calculate_total_mice_for_experiment(
            assigned_ids or [],
            all_cages,
        )

        # Tech resource check
        ok, reason = check_technician_resources_for_period(
            exp_time_min,
            num_mice,
            day_name,
            num_days,
            tech_avail,
            daily_time_booked,
            daily_mice_booked,
            daily_tech_details,
        )
        if not ok:
            return None

        # Drug check (soft — warnings only)
        warnings: list[str] = []
        drug_ok, _, drug_err = check_drug_availability_for_period(
            exp_rec,
            all_manips_map,
            all_drug_inventory,
            all_drugs_map,
            all_cages,
        )
        if drug_err:
            warnings.append(f"Drug: {drug_err}")

        # --- ALL CHECKS PASSED: commit resources ---

        # Commit syringe colors
        if tentative_colors:
            for d_off in range(num_days):
                commit_date = current_date + timedelta(days=d_off)
                for mid, color in tentative_colors.items():
                    daily_used_colors[commit_date].add(color)
                    if color not in daily_color_owners[commit_date]:
                        daily_color_owners[commit_date][color] = mid
                    if mid not in manip_persistent_color:
                        manip_persistent_color[mid] = color

        # Book cages
        if assigned_ids:
            for d_off in range(num_days):
                book_date = current_date + timedelta(days=d_off)
                for cid in assigned_ids:
                    preview_booked_cages[cid].add(book_date)
                dn = DAYS_OF_WEEK_ORDERED[book_date.weekday()]
                daily_mice_booked[dn] += num_mice
                daily_cages_booked[dn] += len(assigned_ids)

            # Washout booking (2 days after experiment)
            manip_customs = set()
            if assignment == "pseudorandom":
                _, ml = parse_notes(original_notes)
                manip_customs = {str(m) for m in (ml or []) if str(m).strip()}
            else:
                for rid in exp_manip_rids:
                    c = manip_rid_to_custom.get(rid)
                    if c:
                        manip_customs.add(c)

            if any(m not in ("m0000000", "m0000004") for m in manip_customs):
                exp_end = current_date + timedelta(
                    days=num_days - 1,
                )
                for wo_day in range(2):
                    wo_date = exp_end + timedelta(days=wo_day + 1)
                    if wo_date in week_dates:
                        for cid in assigned_ids:
                            preview_booked_cages[cid].add(wo_date)

        # Update effective last use
        if assigned_ids and manip_customs_for_color:
            has_injections = any(
                m not in ("m0000000", "m0000004") for m in manip_customs_for_color
            )
            if has_injections:
                cage_idx = {c.get("id"): c for c in all_cages}
                for cid in assigned_ids:
                    cr = cage_idx.get(cid)
                    custom = (cr.get("fields") or {}).get("cage", cid) if cr else cid
                    eff_last_use[str(custom)] = current_date

        # Build custom cage names for display
        custom_names: list[str] = []
        if assigned_ids:
            cage_idx = {c.get("id"): c for c in all_cages}
            for rid in assigned_ids:
                cr = cage_idx.get(rid)
                name = str((cr.get("fields") or {}).get("cage", rid)) if cr else rid
                custom_names.append(name)

        # Build manip display
        display_manips = sorted(manip_customs_for_color)

        end_date = current_date + timedelta(days=num_days)

        return ScheduledExperimentResult(
            record_id=exp_id,
            experiment_id=f.get("experiment_id"),
            assignment=f.get(self.cfg.assignment_field_name, ""),
            priority=f.get("priority", 0),
            num_days=num_days,
            scheduled_start_date=current_date.strftime("%Y-%m-%d"),
            scheduled_end_date=end_date.strftime("%Y-%m-%d"),
            experiment_time_daily=exp_time_min,
            experiment_time_total=exp_time_min * num_days,
            assigned_cages=custom_names,
            assigned_cage_record_ids=assigned_ids or [],
            cage_to_manip_map=(
                {
                    mk: [
                        str(
                            next(
                                (
                                    c.get("fields", {}).get("cage", r)
                                    for c in all_cages
                                    if c.get("id") == r
                                ),
                                r,
                            )
                        )
                        for r in rids
                    ]
                    for mk, rids in (
                        getattr(
                            ExperimentFactory.from_airtable_record(exp_rec),
                            "direct_mapping_map",
                            None,
                        )
                        or {}
                    ).items()
                }
                if assignment == "pseudorandom"
                else {}
            ),
            syringe_colors=tentative_colors,
            manipulation_ids=display_manips,
            notes=final_notes,
            config_file=f.get("config_file", "default_config.json"),
            cages_per_manip=f.get("cages_per_manip"),
            warnings=warnings + violations,
            status=None,
            tasks=f.get("tasks", []),
        )

    @staticmethod
    def _make_result_from_processed(
        p: dict,
        status: str,
    ) -> ScheduledExperimentResult:
        """Convert a processed pre-commit dict to a result."""
        return ScheduledExperimentResult(
            record_id=p.get("record_id", ""),
            experiment_id=p.get("experiment_id_field"),
            assignment=p.get("assignment", ""),
            priority=p.get("priority", 0),
            num_days=int(p.get("num_days", 1) or 1),
            scheduled_start_date=str(p.get("scheduled_start_date", "")),
            scheduled_end_date=str(p.get("scheduled_end_date", "")),
            experiment_time_daily=float(p.get("experiment_time_daily") or 0),
            experiment_time_total=float(p.get("experiment_time_daily") or 0)
            * int(p.get("num_days", 1) or 1),
            assigned_cages=p.get("assigned_cages", []),
            assigned_cage_record_ids=p.get("assigned_cages", []),
            syringe_colors=p.get("assigned_syringe_colors", {}),
            manipulation_ids=p.get(
                "unique_manipulation_ids",
                [],
            ),
            notes=p.get("notes", ""),
            cages_per_manip=p.get("cages_per_manip"),
            status=status,
        )
