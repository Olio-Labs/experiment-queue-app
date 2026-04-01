from __future__ import annotations

from dataclasses import dataclass, field
import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Set

from .enums import AssignmentType, SexAssignmentPreference

logger = logging.getLogger('scheduling_debug')
@dataclass
class ExperimentContext:
    """
    Aggregates read-only data and services needed to perform estimation and
    assignment without passing long argument lists.
    """
    scheduling_date: Optional[date]
    task_times: Dict[str, float]
    cages_pool: List[dict] = field(default_factory=list)
    all_cages: List[dict] = field(default_factory=list)
    boxes: List[dict] = field(default_factory=list)
    planner_history: List[dict] = field(default_factory=list)
    manip_record_id_to_custom_id: Dict[str, str] = field(default_factory=dict)
    all_manipulations_map: Dict[str, dict] = field(default_factory=dict)
    all_drugs_map: Dict[str, dict] = field(default_factory=dict)
    preview_booked_cages: Dict[str, Set[date]] = field(default_factory=dict)
    effective_last_use: Dict[str, date] = field(default_factory=dict)
    cage_to_box_group_map: Dict[str, int] = field(default_factory=dict)


@dataclass
class Experiment:
    """
    Mirrors key fields of the Airtable `experiment_queue` row.
    Subclasses implement assignment/time estimation according to the
    assignment type.
    """
    record_id: str
    title: Optional[str]
    num_days: int
    priority: int
    config_file: Optional[str]
    is_chronic: bool
    notes: str
    assignment: AssignmentType
    # Scheduling-related dates
    earliest_start_date: Optional[date] = None
    actual_start_date: Optional[date] = None
    actual_end_date: Optional[date] = None
    selected_tasks: List[str] = field(default_factory=list)
    experiment_time_minutes: Optional[int] = None

    # assignment-related
    manipulation_ids: List[str] = field(default_factory=list)
    cage_ids: List[str] = field(default_factory=list)

    # mode-specific config
    direct_mapping_map: Optional[Dict[str, List[str]]] = None
    cages_per_manip: Optional[int] = None
    cages_per_vehicle: Optional[int] = None
    sex_assignment: Optional[SexAssignmentPreference] = None

    # --- Behavior ---
    def estimate_minutes(self, ctx: ExperimentContext) -> int:
        raise NotImplementedError

    def assign_cages(self, ctx: ExperimentContext) -> List[str]:
        """
        Assign cages for this experiment. Mutates the instance:
        - updates `direct_mapping_map` and/or `cage_ids`
        - updates `notes` if needed
        Returns a list of warnings (may be empty).
        """
        raise NotImplementedError

    # --- Serialization ---
    def to_airtable_fields(self) -> Dict[str, object]:
        """Serialize domain fields back to Airtable field names."""
        # This is a placeholder to be fleshed out when repositories are added.
        fields: Dict[str, object] = {
            'num_days': self.num_days,
            'priority': self.priority,
            'config_file': self.config_file,
            'is_chronic': self.is_chronic,
            'notes': self.notes,
            'assignment': self.assignment,
            'selected_tasks': self.selected_tasks,
        }
        if self.experiment_time_minutes is not None:
            fields['experiment_time'] = self.experiment_time_minutes
        if self.earliest_start_date is not None:
            fields['earliest_start_date'] = self.earliest_start_date.strftime('%Y-%m-%d')
        if self.actual_start_date is not None:
            fields['actual_start_date'] = self.actual_start_date.strftime('%Y-%m-%d')
        if self.actual_end_date is not None:
            fields['actual_end_date'] = self.actual_end_date.strftime('%Y-%m-%d')
        return fields


class DirectMappingExperiment(Experiment):
    def estimate_minutes(self, ctx: ExperimentContext) -> int:
        # Estimate per-day experiment time using cage types and mice counts from direct mapping
        if self.experiment_time_minutes:
            return int(self.experiment_time_minutes)
        from app.services.time_estimation import estimate_time_direct_mapping_from_notes
        return int(
            (estimate_time_direct_mapping_from_notes(self.notes or '', ctx.task_times or {}, ctx.all_cages or [])[0] or 0)
        )

    def assign_cages(self, ctx: ExperimentContext) -> List[str]:
        from app.services.notes_parser import parse_notes, update_notes_with_mapping
        from app.services.cage_availability_service import is_cage_available_on_date

        warnings: List[str] = []

        # Parse direct mapping from notes
        direct_map, _ = parse_notes(self.notes)
        if not isinstance(direct_map, dict):
            raise ValueError("Direct mapping not found or invalid in notes")

        # Build lookup: custom cage id -> airtable record id from ctx.cages_pool
        custom_to_airtable: Dict[str, str] = {}
        for c in ctx.cages_pool or []:
            cid = c.get('custom_cage_id') or c.get('cage')
            rid = c.get('airtable_record_id') or c.get('id')
            if cid and rid:
                custom_to_airtable[str(cid)] = rid

        # Inflate mapping from custom cage ids to airtable ids
        assignment_map_airtable: Dict[str, List[str]] = {}
        for manip, cage_list in direct_map.items():
            if cage_list is None:
                assignment_map_airtable[str(manip)] = []
                continue
            if not isinstance(cage_list, list):
                cage_list = [cage_list]
            airtable_ids: List[str] = []
            for custom_cage in cage_list:
                rid = custom_to_airtable.get(str(custom_cage))
                if rid:
                    airtable_ids.append(rid)
                else:
                    warnings.append(f"Cage {custom_cage} not found in pool; skipped")
            assignment_map_airtable[str(manip)] = airtable_ids

        # Optional availability check for start date
        start_date = ctx.scheduling_date or date.today()
        final_map: Dict[str, List[str]] = {}
        live_index = {c.get('id'): c for c in (ctx.all_cages or [])}
        for manip, rid_list in assignment_map_airtable.items():
            ok_ids: List[str] = []
            for rid in rid_list:
                rec = live_index.get(rid)
                fields = (rec or {}).get('fields', {})
                booked = ctx.preview_booked_cages.get(rid, set()) if ctx.preview_booked_cages else set()
                try:
                    available = is_cage_available_on_date(
                        fields,
                        rid,
                        start_date,
                        booked,
                        lambda s, f='%Y-%m-%d': datetime.strptime(s, f).date() if isinstance(s, str) else None, # parse
                        '%Y-%m-%d',
                        'manipulations',
                        'm0000000',
                        False,
                        ctx.planner_history or [],
                    )
                except Exception:
                    available = True
                if available:
                    ok_ids.append(rid)
                else:
                    warnings.append(f"Cage {rid} not available on {start_date}")
            final_map[str(manip)] = ok_ids

        # Mutate self
        self.direct_mapping_map = final_map
        self.cage_ids = [rid for lst in final_map.values() for rid in lst]
        try:
            # Update notes using custom cage ids again for readability
            reverse_lookup = {v: k for k, v in custom_to_airtable.items()}
            notes_map: Dict[str, List[str]] = {
                manip: [reverse_lookup.get(rid, rid) for rid in rid_list]
                for manip, rid_list in final_map.items()
            }
            self.notes = update_notes_with_mapping(self.notes, notes_map)
        except Exception:
            pass

        return warnings


class PseudorandomExperiment(Experiment):
    def estimate_minutes(self, ctx: ExperimentContext) -> int:
        if self.experiment_time_minutes:
            return int(self.experiment_time_minutes)
        from app.services.time_estimation import estimate_time_pseudorandom
        # Derive manip list from notes if needed
        manip_ids = list(self.manipulation_ids)
        if not manip_ids and self.notes:
            from app.services.notes_parser import parse_notes
            _, parsed = parse_notes(self.notes)
            if isinstance(parsed, list):
                manip_ids = [str(m) for m in parsed if str(m).strip()]
        cages_per_manip = self.cages_per_manip if isinstance(self.cages_per_manip, int) else 0
        cages_per_vehicle = self.cages_per_vehicle if isinstance(self.cages_per_vehicle, int) else 4
        # Build a mapping custom->record for vehicle detection
        manip_name_to_record_id_map = {}
        for rec_id, custom in (ctx.manip_record_id_to_custom_id or {}).items():
            manip_name_to_record_id_map[str(custom)] = rec_id
        val, _err = estimate_time_pseudorandom(
            cages_per_manip,
            len(manip_ids),
            ctx.task_times or {},
            ctx.all_cages or [],
            manip_ids,
            ctx.all_manipulations_map or {},
            ctx.all_drugs_map or {},
            manip_name_to_record_id_map,
            cages_per_vehicle,
        )
        return int(val or 0)

    def assign_cages(self, ctx: ExperimentContext) -> List[str]:
        from datetime import timedelta
        from app.services.cage_availability_service import (
            select_cages_spatially_with_availability,
            is_cage_available_on_date,
        )
        from app.services.notes_parser import update_notes_with_mapping

        warnings: List[str] = []

        # Determine manipulations to assign
        manips_to_assign = list(self.manipulation_ids)
        if not manips_to_assign and self.notes:
            from app.services.notes_parser import parse_notes
            _m, _list = parse_notes(self.notes)
            manips_to_assign = [m for m in _list]
        manips_to_assign = [m for m in manips_to_assign if str(m).strip()]
        if not manips_to_assign:
            # Nothing to assign
            self.direct_mapping_map = {}
            self.cage_ids = []
            return warnings

        # Detect vehicle manipulations via drugs map
        record_id_by_custom: Dict[str, str] = {}
        for rec_id, custom in (ctx.manip_record_id_to_custom_id or {}).items():
            if custom:
                record_id_by_custom[str(custom)] = rec_id

        vehicle_manips: set[str] = set()
        for manip_custom in manips_to_assign:
            rec_id = record_id_by_custom.get(str(manip_custom))
            if not rec_id:
                continue
            manip_detail = (ctx.all_manipulations_map or {}).get(rec_id)
            if not isinstance(manip_detail, dict):
                continue
            drug_ids = manip_detail.get('fields', {}).get('drugs', []) or []
            for d_id in drug_ids:
                drug_detail = (ctx.all_drugs_map or {}).get(d_id)
                if not isinstance(drug_detail, dict):
                    continue
                drug_types = drug_detail.get('fields', {}).get('drug_type', []) or []
                if isinstance(drug_types, list) and 'vehicle' in drug_types:
                    vehicle_manips.add(str(manip_custom))
                    break

        # Determine counts
        cages_per_manip = self.cages_per_manip if isinstance(self.cages_per_manip, int) else 0
        cages_per_vehicle = self.cages_per_vehicle if isinstance(self.cages_per_vehicle, int) else 4
        sex_pref = self.sex_assignment or 'evenly_split'

        # Build helpers
        def _parse_date(s: object, fmt: str) -> Optional[date]:
            if isinstance(s, str) and s.strip():
                try:
                    from datetime import datetime as _dt
                    return _dt.strptime(s.strip(), fmt).date()
                except Exception:
                    return None
            return None

        AIRTABLE_DATE_FORMAT_STR = '%Y-%m-%d'
        CAGE_MANIP_HISTORY_FIELD = 'manipulations'
        WASHOUT_MANIP_STR = 'm0000000'

        potential_cages_pool = ctx.cages_pool or []
        live_all_cages = ctx.all_cages or []
        planner_history = ctx.planner_history or []
        preview_booked = ctx.preview_booked_cages or {}
        eff_last_use = ctx.effective_last_use or {}

        current_date = ctx.scheduling_date or date.today()
        num_days = int(self.num_days or 1)

        # Partition candidate cages by sex
        male_pool = [c for c in potential_cages_pool if c.get('sex') == 'm']
        female_pool = [c for c in potential_cages_pool if c.get('sex') == 'f']

        assignment_map: Dict[str, List[str]] = {}

        # Build index to access latest cage fields
        live_index: Dict[str, dict] = {c.get('id'): c for c in live_all_cages if c.get('id')}

        def _is_available_all_days(cage_id: str) -> bool:
            rec = live_index.get(cage_id)
            fields = (rec or {}).get('fields', {})
            booked = preview_booked.get(cage_id, set())
            for day_offset in range(num_days):
                check_date = current_date + timedelta(days=day_offset)
                try:
                    ok = is_cage_available_on_date(
                        fields,
                        cage_id,
                        check_date,
                        booked,
                        _parse_date,
                        AIRTABLE_DATE_FORMAT_STR,
                        CAGE_MANIP_HISTORY_FIELD,
                        WASHOUT_MANIP_STR,
                        False,
                        planner_history,
                    )
                except Exception:
                    ok = False
                if not ok:
                    return False
            return True

        for manip in manips_to_assign:
            selected: List[str] = []
            if str(manip) in vehicle_manips:
                need = max(0, cages_per_vehicle)
                if sex_pref == 'male_only':
                    male_need, female_need = need, 0
                elif sex_pref == 'female_only':
                    male_need, female_need = 0, need
                else:
                    male_need = need // 2
                    female_need = need - male_need

                # Diagnostics before selection
                try:
                    avail_m = sum(1 for c in male_pool if _is_available_all_days(c.get('airtable_record_id','')))
                    avail_f = sum(1 for c in female_pool if _is_available_all_days(c.get('airtable_record_id','')))
                except Exception:
                    avail_m, avail_f = len(male_pool), len(female_pool)
                logger.info(f"ASSIGN_DIAG [Vehicle] manip={manip} need M/F={male_need}/{female_need} pool M/F={len(male_pool)}/{len(female_pool)} avail M/F={avail_m}/{avail_f}")

                if male_need:
                    sel_m = select_cages_spatially_with_availability(
                        str(manip), male_need, male_pool, planner_history,
                        current_date, num_days, preview_booked, set(),
                        is_cage_available_on_date, _parse_date, AIRTABLE_DATE_FORMAT_STR,
                        CAGE_MANIP_HISTORY_FIELD, WASHOUT_MANIP_STR, live_all_cages,
                        eff_last_use, ctx.cage_to_box_group_map, set(),
                    )
                    selected.extend(sel_m)
                    male_pool = [c for c in male_pool if c.get('airtable_record_id') not in sel_m]

                if female_need:
                    sel_f = select_cages_spatially_with_availability(
                        str(manip), female_need, female_pool, planner_history,
                        current_date, num_days, preview_booked, set(),
                        is_cage_available_on_date, _parse_date, AIRTABLE_DATE_FORMAT_STR,
                        CAGE_MANIP_HISTORY_FIELD, WASHOUT_MANIP_STR, live_all_cages,
                        eff_last_use, ctx.cage_to_box_group_map, set(),
                    )
                    selected.extend(sel_f)
                    female_pool = [c for c in female_pool if c.get('airtable_record_id') not in sel_f]
                logger.info(f"ASSIGN_DIAG [Vehicle] manip={manip} selected M/F={len([1 for _id in selected if _id in [c.get('airtable_record_id') for c in potential_cages_pool if c.get('sex')=='m']])}/{len([1 for _id in selected if _id in [c.get('airtable_record_id') for c in potential_cages_pool if c.get('sex')=='f']])}")
            else:
                need = max(0, cages_per_manip)
                if sex_pref == 'male_only':
                    male_need, female_need = need, 0
                elif sex_pref == 'female_only':
                    male_need, female_need = 0, need
                else:
                    male_need = need // 2
                    female_need = need - male_need

                # Diagnostics before selection
                try:
                    avail_m = sum(1 for c in male_pool if _is_available_all_days(c.get('airtable_record_id','')))
                    avail_f = sum(1 for c in female_pool if _is_available_all_days(c.get('airtable_record_id','')))
                except Exception:
                    avail_m, avail_f = len(male_pool), len(female_pool)
                logger.info(f"ASSIGN_DIAG [General] manip={manip} need M/F={male_need}/{female_need} pool M/F={len(male_pool)}/{len(female_pool)} avail M/F={avail_m}/{avail_f}")

                if male_need:
                    sel_m = select_cages_spatially_with_availability(
                        str(manip), male_need, male_pool, planner_history,
                        current_date, num_days, preview_booked, set(),
                        is_cage_available_on_date, _parse_date, AIRTABLE_DATE_FORMAT_STR,
                        CAGE_MANIP_HISTORY_FIELD, WASHOUT_MANIP_STR, live_all_cages,
                        eff_last_use, ctx.cage_to_box_group_map, set(),
                    )
                    selected.extend(sel_m)
                    male_pool = [c for c in male_pool if c.get('airtable_record_id') not in sel_m]
                if female_need:
                    sel_f = select_cages_spatially_with_availability(
                        str(manip), female_need, female_pool, planner_history,
                        current_date, num_days, preview_booked, set(),
                        is_cage_available_on_date, _parse_date, AIRTABLE_DATE_FORMAT_STR,
                        CAGE_MANIP_HISTORY_FIELD, WASHOUT_MANIP_STR, live_all_cages,
                        eff_last_use, ctx.cage_to_box_group_map, set(),
                    )
                    selected.extend(sel_f)
                    female_pool = [c for c in female_pool if c.get('airtable_record_id') not in sel_f]
                logger.info(f"ASSIGN_DIAG [General] manip={manip} selected_total={len(selected)} remaining_need={max(0, male_need + female_need - len(selected))}")

            assignment_map[str(manip)] = selected

        # Mutate self
        self.direct_mapping_map = assignment_map
        self.cage_ids = [cid for lst in assignment_map.values() for cid in lst]
        try:
            self.notes = update_notes_with_mapping(self.notes, assignment_map)
        except Exception:
            logging.getLogger('domain.experiment').warning(
                "Failed to update notes with direct mapping; leaving notes unchanged.",
                exc_info=True,
            )

        return warnings


class ExperimentFactory:
    @staticmethod
    def from_airtable_record(record: dict) -> Experiment:
        """
        Construct an Experiment (and appropriate subclass) from an Airtable
        record dict. This uses a best-effort mapping for now; it will be
        refined as repositories are added.
        """
        fields = record.get('fields', {})

        assignment: AssignmentType = fields.get('assignment')  # type: ignore
        if not assignment:
            raise ValueError(f"Experiment {record.get('id', 'unknown')} is missing required 'assignment' field")
        # Parse scheduling fields
        def _parse_optional_date(value: object) -> Optional[date]:
            if isinstance(value, str) and value.strip():
                try:
                    return datetime.strptime(value.strip(), '%Y-%m-%d').date()
                except Exception:
                    return None
            return None

        parsed_earliest = _parse_optional_date(fields.get('earliest_start_date') or fields.get('start_date'))
        parsed_actual_start = _parse_optional_date(fields.get('actual_start_date'))
        parsed_actual_end = _parse_optional_date(fields.get('actual_end_date'))

        base_kwargs = dict(
            record_id=record.get('id', ''),
            title=fields.get('experiment_name') or fields.get('name'),
            num_days=int(fields.get('num_days') or fields.get('start_days') or 0),
            priority=int(fields.get('priority') or 0),
            config_file=fields.get('config_file'),
            is_chronic=bool(fields.get('is_chronic') or False),
            notes=fields.get('notes') or '',
            assignment=assignment,
            earliest_start_date=parsed_earliest,
            actual_start_date=parsed_actual_start,
            actual_end_date=parsed_actual_end,
            selected_tasks=fields.get('selected_tasks') or [],
            experiment_time_minutes=fields.get('experiment_time'),
            manipulation_ids=[],
            cage_ids=[],
        )

        if assignment == 'pseudorandom':
            return PseudorandomExperiment(
                **base_kwargs,
                cages_per_manip=_safe_int(fields.get('cages_per_manip')),
                cages_per_vehicle=_safe_int(fields.get('cages_per_vehicle')),
                sex_assignment=fields.get('sex_assignment') or 'evenly_split',
            )
        else:
            # direct mapping experiments may contain mapping in notes; that parsing
            # will be performed by services and set onto the instance later.
            return DirectMappingExperiment(**base_kwargs)


def _safe_int(value: object, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


