import csv
import gzip
import hashlib
import io
import re
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import boto3
import cv2
import matplotlib
import pandas as pd
from pyairtable import Api

matplotlib.use("Agg")  # Use non-interactive backend
import base64
import logging
import os
import time

import matplotlib.pyplot as plt

from ..config import settings

logger = logging.getLogger(__name__)


def _normalize_box_id(raw_box_id: Any) -> str:
    """
    Coerce lookup 'box_id' to a normalized zero-padded string like 'b0000041'.
    Accepts a single string or a list from Airtable lookup. Returns '' if invalid.
    """
    if raw_box_id is None:
        return ""
    # Lookup fields often return a list
    if isinstance(raw_box_id, list):
        raw_box_id = raw_box_id[0] if raw_box_id else ""
    try:
        s = str(raw_box_id).strip()
        if not s:
            return ""
        if s.startswith("b") and len(s) >= 2 and s[1:].isdigit():
            num = int(s[1:])
            return f"b{num:07d}"
        if s.isdigit():
            return f"b{int(s):07d}"
        return s
    except Exception:
        return ""


def _box_number_from_box_id(box_id: str) -> int:
    try:
        return int(box_id[1:]) if box_id and box_id.startswith("b") else -1
    except Exception:
        return -1


_CACHE_TTL_SECONDS_DEFAULT = 60
# Cache keyed by date string 'YYYY-MM-DD'
# -> (ts, (overlays_by_box_number, overlay_errors))
_overlays_by_date_cache: Dict[
    str, Tuple[float, Tuple[Dict[int, List[Dict[str, Any]]], List[str]]]
] = {}
# Cache keyed by (box_number, date_str) -> (ts, history_list)
_box_history_cache: Dict[Tuple[int, str], Tuple[float, List[Dict[str, Any]]]] = {}


def _parse_yyyy_mm_dd(date_str: str) -> datetime.date:
    """
    Parse and validate a date string in YYYY-MM-DD format.
    Raises ValueError if invalid.
    """
    try:
        return datetime.strptime(str(date_str).strip(), "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Invalid date '{date_str}'. Expected YYYY-MM-DD.")


def _chunked(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    return [items[i : i + size] for i in range(0, len(items), size)]


_CSS_COLOR_ALLOWLIST: Set[str] = {
    "red",
    "blue",
    "green",
    "yellow",
    "orange",
    "purple",
    "pink",
    "black",
    "white",
    "gray",
    "grey",
    "brown",
    "cyan",
    "magenta",
    "teal",
    "navy",
    "maroon",
    "olive",
    "silver",
    "gold",
    "peach",
    "lightgreen",
    "lightblue",
}

_CSS_COLOR_ALIASES: Dict[str, str] = {
    # 'peach' is not a standard CSS named color; map to a standard equivalent.
    # Reference: CSS supports 'peachpuff' (and many others) but not 'peach'.
    "peach": "#FFDAB9",  # peachpuff
    # Pattern token base colors
    "hearts": "#ffb6c1",  # lightpink
}


def _sanitize_syringe_color_css(raw_color: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Fail-loud sanitizer: only allow hex (#RGB/#RRGGBB) or a small named-color allowlist.
    Returns (css_color_or_None, error_or_None).
    """
    if raw_color is None:
        return None, None
    s = str(raw_color).strip()
    if not s:
        return None, None
    if re.fullmatch(r"#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})", s):
        return s, None
    s_norm = s.strip().lower()
    # For labels like "pink_tribal" or "white_confetti", take the base color.
    if "_" in s_norm:
        s_norm = s_norm.split("_", 1)[0]
    if s_norm in _CSS_COLOR_ALIASES:
        return _CSS_COLOR_ALIASES[s_norm], None
    if s_norm in _CSS_COLOR_ALLOWLIST:
        return s_norm, None
    # User preference: if it's not a real CSS color,
    # just don't show a stripe (no error).
    return None, None


def fetch_box_overlays_for_date(
    api_key: str,
    base_id: str,
    date_str: str,
    experiment_id_filter: Optional[str] = None,
) -> Tuple[Dict[int, List[Dict[str, Any]]], List[str]]:
    """
    Fetch experiment_planner rows for a specific date and
    aggregate per box_number overlays of
    (manipulation_id, syringe_color).

    Returns:
        overlays_by_box_number: {box_number: [{manipulation_id,
            syringe_color, syringe_color_css}, ...]}
        overlay_errors: list of human-readable error strings
            (shown in UI; not silent)
    """
    date_str = str(date_str).strip()
    _parse_yyyy_mm_dd(date_str)

    exp_filter = (experiment_id_filter or "").strip()

    # Lightweight cache keyed by date. Keep TTL short to
    # avoid hiding changes while still avoiding repeated
    # reads per request burst.
    now = time.time()
    cache_key = date_str if not exp_filter else f"{date_str}|exp:{exp_filter}"
    cached = _overlays_by_date_cache.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL_SECONDS_DEFAULT:
        return cached[1][0], cached[1][1]

    overlay_errors: List[str] = []
    overlays_by_box_number: Dict[int, List[Dict[str, Any]]] = {}

    if not api_key or not base_id:
        msg = (
            "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID;"
            " cannot load manipulation/syringe overlays."
        )
        overlay_errors.append(msg)
        return overlays_by_box_number, overlay_errors

    try:
        api = Api(api_key)

        # Map manipulation record_id -> custom manipulation_id (e.g. m0000004)
        manip_record_id_to_custom: Dict[str, str] = {}
        try:
            manip_table = api.table(base_id, "manipulations")
            manip_records = manip_table.all(fields=["manipulation"])
            for rec in manip_records:
                rid = rec.get("id")
                custom = (rec.get("fields", {}) or {}).get("manipulation")
                if rid and custom:
                    manip_record_id_to_custom[str(rid)] = str(custom)
        except Exception as e:
            overlay_errors.append(
                f"Could not load manipulations table to map manipulation IDs: {e}"
            )
            logger.exception("Box room overlay: failed to build manipulation id map")
            manip_record_id_to_custom = {}

        planner_table = api.table(base_id, "experiment_planner")
        formula = f"IS_SAME({{start_date}}, '{date_str}', 'day')"
        if exp_filter:
            formula = f"AND({formula}, {{experiment_id}} = '{exp_filter}')"
        fields = [
            "start_date",
            "experiment_id",
            "box_id",
            "manipulation_",
            "syringe_color",
        ]
        planner_records = planner_table.all(formula=formula, fields=fields)

        seen_by_box: Dict[int, Set[Tuple[str, str]]] = {}

        for rec in planner_records:
            rec_id = rec.get("id", "unknown_record")
            f = rec.get("fields", {}) or {}

            box_id = _normalize_box_id(f.get("box_id"))
            if not box_id:
                msg = (
                    f"experiment_planner record {rec_id}:"
                    f" missing/invalid box_id"
                    f" for date {date_str}."
                )
                overlay_errors.append(msg)
                continue
            box_number = _box_number_from_box_id(box_id)
            if box_number < 0:
                msg = (
                    f"experiment_planner record {rec_id}:"
                    f" could not parse box_number"
                    f" from box_id '{box_id}'."
                )
                overlay_errors.append(msg)
                continue

            manip_links = f.get("manipulation_") or []
            if isinstance(manip_links, str):
                manip_links = [manip_links]
            if not isinstance(manip_links, list) or not manip_links:
                msg = (
                    f"experiment_planner record {rec_id}:"
                    f" missing manipulation_ for box"
                    f" {box_id} on date {date_str}."
                )
                overlay_errors.append(msg)
                manip_links = []

            syringe_raw = f.get("syringe_color")
            colors: List[Any]
            if syringe_raw is None:
                colors = []
            elif isinstance(syringe_raw, list):
                colors = [
                    c for c in syringe_raw if c is not None and str(c).strip() != ""
                ]
            else:
                colors = [syringe_raw] if str(syringe_raw).strip() != "" else []

            overlays_by_box_number.setdefault(box_number, [])
            seen_by_box.setdefault(box_number, set())

            if not manip_links:
                # Still render an "unknown" segment so the box
                # visibly indicates something is missing.
                for raw_color in colors:
                    raw_token = (
                        str(raw_color).strip().lower() if raw_color is not None else ""
                    )
                    css_color, color_err = _sanitize_syringe_color_css(raw_color)
                    key = ("UNKNOWN", str(raw_color) if raw_color is not None else "")
                    if key in seen_by_box[box_number]:
                        continue
                    seen_by_box[box_number].add(key)
                    overlays_by_box_number[box_number].append(
                        {
                            "manipulation_id": "UNKNOWN",
                            "syringe_color": raw_color,
                            "syringe_color_css": css_color,
                            "syringe_color_pattern": "hearts"
                            if raw_token == "hearts"
                            else None,
                        }
                    )
                continue

            for manip_record_id in manip_links:
                manip_custom = manip_record_id_to_custom.get(str(manip_record_id))
                if not manip_custom:
                    msg = (
                        f"experiment_planner record"
                        f" {rec_id}: could not map"
                        f" manipulation_ record id to"
                        f" custom manipulation_id for"
                        f" box {box_id} on"
                        f" date {date_str}."
                    )
                    overlay_errors.append(msg)
                    manip_custom = "UNKNOWN"

                for raw_color in colors:
                    raw_token = (
                        str(raw_color).strip().lower() if raw_color is not None else ""
                    )
                    css_color, color_err = _sanitize_syringe_color_css(raw_color)
                    key = (
                        manip_custom,
                        str(raw_color) if raw_color is not None else "",
                    )
                    if key in seen_by_box[box_number]:
                        continue
                    seen_by_box[box_number].add(key)
                    overlays_by_box_number[box_number].append(
                        {
                            "manipulation_id": manip_custom,
                            "syringe_color": raw_color,
                            "syringe_color_css": css_color,
                            "syringe_color_pattern": "hearts"
                            if raw_token == "hearts"
                            else None,
                        }
                    )

        # Stable sort for rendering
        for bn in overlays_by_box_number.keys():
            overlays_by_box_number[bn].sort(
                key=lambda o: (
                    str(o.get("manipulation_id") or ""),
                    str(o.get("syringe_color") or ""),
                )
            )

    except Exception as e:
        overlay_errors.append(
            f"Error loading box overlays from experiment_planner ({date_str}): {e}"
        )
        logger.exception("Box room overlay: failed to load overlays")

    _overlays_by_date_cache[cache_key] = (now, (overlays_by_box_number, overlay_errors))
    return overlays_by_box_number, overlay_errors


def fetch_box_room_data(
    api_key: str,
    base_id: str,
    start_date: Optional[str] = None,
    experiment_id_filter: Optional[str] = None,
) -> Tuple[Dict[int, Dict], List[int], List[str]]:
    """
    Fetch alive cages with non-empty box_id and aggregate per box.
    Also loads experiments_planner data for flagged_issues_history.

    Returns:
        boxes_by_number: { box_number: {
            'box_id': str,
            'male_count': int,
            'female_count': int,
            'cages': List[str],
            'issues': List[str],  # unique, excluding 'None'
            'issues_history_present': bool,
            'today_overlays': List[dict]  # optional;
                # for today's manipulation/syringe stripes
        }}
        boxes_with_issues_sorted: List[int]  # box_numbers with issues
        overlay_errors: List[str]  # visible errors for missing/invalid overlay data
    """
    if not api_key or not base_id:
        raise ValueError("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID")

    date_str = (start_date or datetime.now().date().strftime("%Y-%m-%d")).strip()
    _parse_yyyy_mm_dd(date_str)

    exp_filter = (experiment_id_filter or "").strip()

    # Use experiment_planner (selected day) as the source of
    # truth for which cages were in which boxes.
    planner_table = Api(api_key).table(base_id, "experiment_planner")
    planner_formula = f"IS_SAME({{start_date}}, '{date_str}', 'day')"
    if exp_filter:
        planner_formula = f"AND({planner_formula}, {{experiment_id}} = '{exp_filter}')"
    # box_id is typically a lookup field; box_ / cage_ are linked record fields.
    planner_fields = ["start_date", "experiment_id", "box_id", "box_", "cage_"]
    planner_records = planner_table.all(formula=planner_formula, fields=planner_fields)

    boxes_by_number: Dict[int, Dict] = {}
    box_number_to_cage_record_ids: Dict[int, Set[str]] = {}
    missing_box_id_box_record_ids: Set[str] = set()

    for rec in planner_records:
        f = rec.get("fields", {}) or {}
        raw_box_id = f.get("box_id")
        box_id = _normalize_box_id(raw_box_id)
        if not box_id:
            # Fallback: if we have a linked box_ record id,
            # resolve box_id via the boxes table later.
            box_links = f.get("box_") or []
            if isinstance(box_links, str):
                box_links = [box_links]
            if isinstance(box_links, list):
                for bid in box_links:
                    if bid:
                        missing_box_id_box_record_ids.add(str(bid))
            continue

        box_number = _box_number_from_box_id(box_id)
        if box_number < 0:
            continue

        cage_links = f.get("cage_") or []
        if isinstance(cage_links, str):
            cage_links = [cage_links]

        if box_number not in boxes_by_number:
            boxes_by_number[box_number] = {
                "box_id": box_id,
                "male_count": 0,
                "female_count": 0,
                "cages": [],
                "issues": set(),
                "issues_history_present": False,
            }

        box_number_to_cage_record_ids.setdefault(box_number, set())
        if isinstance(cage_links, list):
            for cid in cage_links:
                if cid:
                    box_number_to_cage_record_ids[box_number].add(str(cid))

    # Resolve any missing box_id via box_ links (rare, but we fail less mysteriously).
    if missing_box_id_box_record_ids:
        boxes_table = Api(api_key).table(base_id, "boxes")
        # Chunk to avoid overly-long formulas.
        for chunk in _chunked(sorted(missing_box_id_box_record_ids), 20):
            formula = (
                "OR(" + ", ".join([f"RECORD_ID() = '{rid}'" for rid in chunk]) + ")"
            )
            try:
                recs = boxes_table.all(formula=formula, fields=["box_id"])
                for r in recs:
                    ff = r.get("fields", {}) or {}
                    box_id = _normalize_box_id(ff.get("box_id"))
                    if not box_id:
                        continue
                    bn = _box_number_from_box_id(box_id)
                    if bn < 0:
                        continue
                    boxes_by_number.setdefault(
                        bn,
                        {
                            "box_id": box_id,
                            "male_count": 0,
                            "female_count": 0,
                            "cages": [],
                            "issues": set(),
                            "issues_history_present": False,
                        },
                    )
            except Exception:
                logger.exception(
                    "Failed to resolve box_id from boxes table via box_ links"
                )

    # Hydrate cages from cages table using linked record ids.
    cage_record_ids: List[str] = sorted(
        {cid for s in box_number_to_cage_record_ids.values() for cid in s}
    )
    cage_by_record_id: Dict[str, Dict[str, Any]] = {}
    if cage_record_ids:
        cages_table = Api(api_key).table(base_id, "cages")
        cage_fields = [
            "cage",
            "sex",
            "n_mice",
            "flagged_issues",
            "flagged_issues_history",
        ]
        for chunk in _chunked(cage_record_ids, 20):
            formula = (
                "OR(" + ", ".join([f"RECORD_ID() = '{rid}'" for rid in chunk]) + ")"
            )
            recs = cages_table.all(formula=formula, fields=cage_fields)
            for r in recs:
                rid = r.get("id")
                if not rid:
                    continue
                cage_by_record_id[str(rid)] = r.get("fields", {}) or {}

    # Aggregate per box from hydrated cages.
    for bn, cage_rids in box_number_to_cage_record_ids.items():
        entry = boxes_by_number.get(bn)
        if not entry:
            continue
        for rid in cage_rids:
            f = cage_by_record_id.get(rid) or {}
            sex_value = (f.get("sex") or "").strip().lower()
            try:
                n_mice = int(f.get("n_mice") or 0)
            except Exception:
                try:
                    n_mice = int(float(str(f.get("n_mice"))))
                except Exception:
                    n_mice = 0

            cage_custom_id = str(f.get("cage")) if f.get("cage") is not None else None
            if cage_custom_id:
                entry["cages"].append(cage_custom_id)

            if sex_value == "m":
                entry["male_count"] += n_mice
            elif sex_value == "f":
                entry["female_count"] += n_mice

            flagged_issues_raw = f.get("flagged_issues") or []
            if isinstance(flagged_issues_raw, str):
                flagged_issues_list = [flagged_issues_raw]
            else:
                flagged_issues_list = [
                    str(v) for v in flagged_issues_raw if v is not None
                ]
            filtered_issues = [
                iss for iss in flagged_issues_list if iss.strip().lower() != "none"
            ]
            for iss in filtered_issues:
                if iss:
                    entry["issues"].add(iss)

            if str(f.get("flagged_issues_history") or "").strip():
                entry["issues_history_present"] = True

    # Convert issues sets to lists and compute issue boxes
    boxes_with_issues: List[int] = []
    for bn, entry in boxes_by_number.items():
        entry["issues"] = sorted(list(entry["issues"]))
        if entry["issues"]:
            boxes_with_issues.append(bn)

    boxes_with_issues_sorted = sorted(boxes_with_issues)

    # Add per-box overlays for the selected date (manipulation_id + syringe_color)
    overlays_by_box_number, overlay_errors = fetch_box_overlays_for_date(
        api_key, base_id, date_str, exp_filter or None
    )
    for bn, overlays in overlays_by_box_number.items():
        if bn not in boxes_by_number:
            # Create minimal entry so empty boxes can still show today's stripes
            boxes_by_number[bn] = {
                "box_id": f"b{bn:07d}",
                "male_count": 0,
                "female_count": 0,
                "cages": [],
                "issues": [],
                "issues_history_present": False,
            }
        boxes_by_number[bn]["today_overlays"] = overlays
        boxes_by_number[bn]["today_overlays_css"] = [
            o for o in overlays if o.get("syringe_color_css")
        ]

    return boxes_by_number, boxes_with_issues_sorted, overlay_errors


def fetch_cages_without_box_data(
    api_key: str, base_id: str, filter_taconic_only: bool = True
) -> Tuple[List[Dict], List[str]]:
    """
    Fetch alive cages without box_id assigned and their flagged issues.

    Args:
        filter_taconic_only: If True, only return cages with
            'bought_from' containing 'taconic'

    Returns:
        cages_data: List of cage dictionaries with cage_id, sex, n_mice, flagged_issues
        cages_with_issues: List of cage IDs that have flagged issues
    """
    if not api_key or not base_id:
        raise ValueError("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID")

    table = Api(api_key).table(base_id, "cages")

    base_formula = "AND({alive}='True', LEN(ARRAYJOIN({box_id}))=0)"
    if filter_taconic_only:
        taconic_formula = "FIND('taconic', LOWER(ARRAYJOIN({bought_from})))>0"
        formula = f"AND({base_formula}, {taconic_formula})"
    else:
        formula = base_formula

    fields = [
        "cage",
        "sex",
        "n_mice",
        "flagged_issues",
        "flagged_issues_history",
        "alive",
        "box_id",
        "bought_from",
    ]
    records = table.all(formula=formula, fields=fields)

    cages_data = []
    cages_with_issues = []

    for rec in records:
        f = rec.get("fields", {})
        cage_id = str(f.get("cage")) if f.get("cage") is not None else None
        if not cage_id:
            continue

        sex_value = (f.get("sex") or "").strip().lower()
        try:
            n_mice = int(f.get("n_mice") or 0)
        except Exception:
            try:
                n_mice = int(float(str(f.get("n_mice"))))
            except Exception:
                n_mice = 0

        flagged_issues_raw = f.get("flagged_issues") or []
        if isinstance(flagged_issues_raw, str):
            flagged_issues_list = [flagged_issues_raw]
        else:
            flagged_issues_list = [str(v) for v in flagged_issues_raw if v is not None]

        # Exclude 'None' (case-insensitive)
        filtered_issues = [
            iss for iss in flagged_issues_list if iss.strip().lower() != "none"
        ]

        cage_data = {
            "cage_id": cage_id,
            "sex": sex_value,
            "n_mice": n_mice,
            "issues": filtered_issues,
            "issues_history_present": bool(
                str(f.get("flagged_issues_history") or "").strip()
            ),
        }

        cages_data.append(cage_data)

        if filtered_issues:
            cages_with_issues.append(cage_id)

    # Sort cages by cage_id
    cages_data.sort(key=lambda x: x["cage_id"])

    return cages_data, cages_with_issues


def get_box_flagged_issues_history(
    box_number: int,
    start_date: Optional[str] = None,
    experiment_id_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch flagged-issues history for a specific box from experiment_planner on-demand.
    Returns list of entries with cage_id, start_date, and flagged_issues.
    """
    if box_number <= 0:
        raise ValueError(f"Invalid box_number: {box_number}")

    now = time.time()
    date_str = (start_date or datetime.now().date().strftime("%Y-%m-%d")).strip()
    _parse_yyyy_mm_dd(date_str)

    cached = _box_history_cache.get((box_number, date_str))
    if cached and (now - cached[0]) < _CACHE_TTL_SECONDS_DEFAULT:
        return cached[1]

    api_key = settings.airtable_api_key
    base_id = settings.airtable_base_id
    if not api_key or not base_id:
        raise ValueError("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured")

    target_box_id = f"b{box_number:07d}"
    table = Api(api_key).table(base_id, "experiment_planner")

    # For a date-scoped box room view, keep history scoped to the selected day.
    base_formula = (
        f"AND({{box_id}} = '{target_box_id}',"
        f" IS_SAME({{start_date}}, '{date_str}', 'day'))"
    )
    exp_filter = (experiment_id_filter or "").strip()
    formula = (
        f"AND({base_formula}, {{experiment_id}} = '{exp_filter}')"
        if exp_filter
        else base_formula
    )

    fields = ["cage_id", "start_date", "experiment_id", "Flagged issues"]
    records = table.all(formula=formula, fields=fields)

    history: List[Dict[str, Any]] = []
    for rec in records:
        f = rec.get("fields", {}) or {}
        cage_id = f.get("cage_id")
        if isinstance(cage_id, list) and cage_id:
            cage_id = cage_id[0]
        history.append(
            {
                "cage_id": cage_id or "Unknown",
                "start_date": f.get("start_date"),
                "flagged_issues": f.get("Flagged issues"),
            }
        )

    def sort_key(entry: Dict[str, Any]) -> str:
        sd = entry.get("start_date")
        if isinstance(sd, list) and sd:
            return str(sd[0])
        return str(sd or "")

    history.sort(key=sort_key)
    _box_history_cache[(box_number, date_str)] = (now, history)
    return history


def get_cage_flagged_issues_history(cage_id: str) -> str:
    """
    Get flagged_issues_history for a specific cage from the cages table.
    Returns the flagged_issues_history text field content.
    """
    try:
        api_key = settings.airtable_api_key
        base_id = settings.airtable_base_id

        if not api_key or not base_id:
            logger.error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured")
            return ""

        table = Api(api_key).table(base_id, "cages")

        # Filter by cage_id
        formula = f"{{cage}} = '{cage_id}'"
        fields = ["flagged_issues_history"]

        records = table.all(formula=formula, fields=fields)

        if records and len(records) > 0:
            fields_data = records[0].get("fields", {})
            return str(fields_data.get("flagged_issues_history") or "").strip()

        return ""

    except Exception as e:
        print(f"Error fetching cage flagged issues history for {cage_id}: {e}")
        return ""


def get_experiment_ids_for_start_date(
    api_key: str, base_id: str, date_str: str
) -> Tuple[List[str], Optional[str]]:
    """
    Return unique experiment_id values from experiment_planner
    for a given start_date (YYYY-MM-DD).
    Used for displaying a "today's experiment" id in the UI.

    Returns (experiment_ids_sorted, error_or_None).
    Never raises for missing/multiple; caller decides UI.
    """
    try:
        date_str = str(date_str).strip()
        _parse_yyyy_mm_dd(date_str)
    except Exception as e:
        return [], str(e)

    if not api_key or not base_id:
        return [], "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID"

    try:
        table = Api(api_key).table(base_id, "experiment_planner")
        formula = f"IS_SAME({{start_date}}, '{date_str}', 'day')"
        records = table.all(formula=formula, fields=["experiment_id"])
        ids: Set[str] = set()
        for rec in records:
            f = rec.get("fields", {}) or {}
            exp_id = f.get("experiment_id")
            if isinstance(exp_id, list) and exp_id:
                exp_id = exp_id[0]
            if exp_id:
                ids.add(str(exp_id).strip())
        ids_sorted = sorted([i for i in ids if i])
        if not ids_sorted:
            return [], f"No experiment_id found for start_date {date_str}"
        if len(ids_sorted) > 1:
            return (
                ids_sorted,
                "Multiple experiment_id values for"
                f" start_date {date_str}:"
                f" {', '.join(ids_sorted)}",
            )
        return ids_sorted, None
    except Exception as e:
        return [], f"Error loading experiment_id for start_date {date_str}: {e}"


def get_start_date_for_experiment_id(
    api_key: str, base_id: str, experiment_id: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve the unique start_date (YYYY-MM-DD) for a given
    experiment_id by querying experiment_planner.
    Returns (start_date_str_or_None, error_or_None).
    """
    exp = str(experiment_id or "").strip()
    if not exp:
        return None, "Missing experiment_id"
    if not api_key or not base_id:
        return None, "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID"

    try:
        table = Api(api_key).table(base_id, "experiment_planner")
        formula = f"{{experiment_id}} = '{exp}'"
        records = table.all(formula=formula, fields=["experiment_id", "start_date"])
        dates: Set[str] = set()
        for rec in records:
            f = rec.get("fields", {}) or {}
            sd = f.get("start_date")
            if isinstance(sd, list) and sd:
                sd = sd[0]
            if sd:
                dates.add(str(sd).strip())
        dates_sorted = sorted([d for d in dates if d])
        if not dates_sorted:
            return None, f"No start_date found for experiment_id {exp}"
        if len(dates_sorted) > 1:
            return (
                None,
                "Multiple start_date values for"
                f" experiment_id {exp}:"
                f" {', '.join(dates_sorted)}",
            )
        _parse_yyyy_mm_dd(dates_sorted[0])
        return dates_sorted[0], None
    except Exception as e:
        return None, f"Error resolving start_date for experiment_id {exp}: {e}"


def _try_find_recent_video(
    s3_client,
    experiment_id: str,
    box_id: str,
    cage_id: str,
    s3_bucket: str,
    time_limit: Optional[timedelta],
) -> Optional[Dict[str, Any]]:
    """
    Helper function to try finding a video for a specific experiment.

    Returns dict with video info if found, None otherwise.
    """
    # Construct S3 prefix path
    s3_prefix = f"{experiment_id}/{box_id}/{cage_id}/"

    print(f"Looking for videos in S3: s3://{s3_bucket}/{s3_prefix}")

    # List objects in S3
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return None

    if "Contents" not in response:
        print(f"No files found in S3 path: {s3_prefix}")
        return None

    # Filter for usbcam-0.mp4 files
    video_files = []
    pattern = re.compile(rf"{experiment_id}_(\d{{14}})_usbcam-0\.mp4$")

    for obj in response["Contents"]:
        key = obj["Key"]
        filename = key.split("/")[-1]
        match = pattern.match(filename)
        if match:
            timestamp_str = match.group(1)
            try:
                # Parse timestamp: YYYYMMDDHHMMSS
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                video_files.append(
                    {"key": key, "timestamp": timestamp, "timestamp_str": timestamp_str}
                )
            except ValueError:
                print(f"Could not parse timestamp from: {filename}")
                continue

    if not video_files:
        print("No usbcam-0.mp4 files found matching pattern")
        return None

    # Optionally filter for videos within time limit
    candidates = video_files
    if time_limit is not None:
        now = datetime.now()
        time_limit_ago = now - time_limit
        candidates = [v for v in video_files if v["timestamp"] >= time_limit_ago]
        if not candidates:
            print("No videos found within time limit")
            return None

    # Sort by timestamp (most recent first) and get the most recent
    candidates.sort(key=lambda x: x["timestamp"], reverse=True)
    most_recent = candidates[0]

    print(f"Found most recent video: {most_recent['key']}")

    # Look for corresponding CO2 CSV file
    co2_csv_key = None
    timestamp_str = most_recent["timestamp_str"]
    for obj in response["Contents"]:
        key = obj["Key"]
        filename = key.split("/")[-1]
        if filename == f"{experiment_id}_{timestamp_str}_co2-sensor-scd40-0.csv.gz":
            co2_csv_key = key
            print(f"Found corresponding CO2 CSV: {co2_csv_key}")
            break

    return {
        "key": most_recent["key"],
        "timestamp_str": most_recent["timestamp_str"],
        "experiment_id": experiment_id,
        "co2_csv_key": co2_csv_key,
    }


def _try_find_recent_video_in_hour(
    s3_client,
    experiment_id: str,
    box_id: str,
    cage_id: str,
    s3_bucket: str,
    target_date: datetime.date,
    target_hour: int,
) -> Optional[Dict[str, Any]]:
    """
    Find the most recent usbcam-0.mp4 for a given
    experiment/box/cage whose timestamp (from filename)
    falls on target_date at target_hour
    (local time as encoded in the filename).
    """
    if target_hour < 0 or target_hour > 23:
        raise ValueError(f"Invalid target_hour: {target_hour}")

    s3_prefix = f"{experiment_id}/{box_id}/{cage_id}/"
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return None

    if "Contents" not in response:
        return None

    pattern = re.compile(rf"{re.escape(experiment_id)}_(\d{{14}})_usbcam-0\.mp4$")
    candidates: List[Dict[str, Any]] = []

    for obj in response["Contents"]:
        key = obj.get("Key") or ""
        filename = key.split("/")[-1]
        match = pattern.match(filename)
        if not match:
            continue
        timestamp_str = match.group(1)
        try:
            ts = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        except ValueError:
            continue
        if ts.date() != target_date or ts.hour != target_hour:
            continue
        candidates.append({"key": key, "timestamp": ts, "timestamp_str": timestamp_str})

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["timestamp"], reverse=True)
    most_recent = candidates[0]

    co2_csv_key = None
    for obj in response["Contents"]:
        key = obj.get("Key") or ""
        filename = key.split("/")[-1]
        if (
            filename == f"{experiment_id}_{most_recent['timestamp_str']}"
            "_co2-sensor-scd40-0.csv.gz"
        ):
            co2_csv_key = key
            break

    return {
        "key": most_recent["key"],
        "timestamp_str": most_recent["timestamp_str"],
        "experiment_id": experiment_id,
        "co2_csv_key": co2_csv_key,
    }


def _list_usbcam_videos_for_experiment_window(
    s3_client,
    experiment_id: str,
    box_id: str,
    cage_id: str,
    s3_bucket: str,
    start_date_str: str,
) -> List[Dict[str, Any]]:
    """
    List usbcam-0.mp4 objects for the given
    experiment/box/cage and filter to the experiment window:
    3pm PST on start_date through 3pm PST the next day.

    The filename timestamps are treated as PST-local wall time (per user assumption).
    Returns sorted list (most recent first): {key, timestamp_str, timestamp_dt}.
    """
    start_date_str = str(start_date_str).strip()
    _parse_yyyy_mm_dd(start_date_str)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    window_start = datetime.combine(start_date, datetime.min.time()).replace(
        hour=15, minute=0, second=0
    )
    window_end = window_start + timedelta(days=1)

    s3_prefix = f"{experiment_id}/{box_id}/{cage_id}/"
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    except Exception as e:
        raise RuntimeError(
            f"Error listing S3 objects under s3://{s3_bucket}/{s3_prefix}: {e}"
        )

    if "Contents" not in response:
        return []

    pattern = re.compile(rf"{re.escape(experiment_id)}_(\d{{14}})_usbcam-0\.mp4$")
    videos: List[Dict[str, Any]] = []
    for obj in response["Contents"]:
        key = obj.get("Key") or ""
        filename = key.split("/")[-1]
        m = pattern.match(filename)
        if not m:
            continue
        ts_str = m.group(1)
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
        except ValueError:
            continue
        if ts < window_start or ts >= window_end:
            continue
        videos.append({"key": key, "timestamp_str": ts_str, "timestamp_dt": ts})

    videos.sort(key=lambda x: x["timestamp_dt"], reverse=True)
    return videos


def _format_timestamp_label_pst(ts_str: str) -> str:
    """
    Make YYYYMMDDHHMMSS human-readable for the dropdown.
    """
    try:
        dt = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S PST")
    except Exception:
        return ts_str


def _generate_co2_plot(s3_client, co2_csv_key: str, s3_bucket: str) -> Optional[str]:
    """
    Download CO2 CSV file from S3, parse it, and generate a plot.
    Returns base64-encoded PNG image string or None if failed.
    """
    try:
        # Download the gzipped CSV file
        response = s3_client.get_object(Bucket=s3_bucket, Key=co2_csv_key)
        gzipped_data = response["Body"].read()

        # Decompress and read CSV
        csv_data = gzip.decompress(gzipped_data).decode("utf-8")
        df = pd.read_csv(io.StringIO(csv_data), index_col=0, parse_dates=True)

        # Create plot with subplots
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
        fig.suptitle("CO2 Sensor Data", fontsize=14)

        # Plot CO2
        ax1.plot(df.index, df["co2_raw"], "b-", linewidth=1)
        ax1.set_ylabel("CO2 (ppm)", fontsize=10)
        ax1.set_title("CO2 Concentration", fontsize=12)
        ax1.grid(True, alpha=0.3)

        # Plot Temperature
        ax2.plot(df.index, df["temperature"], "r-", linewidth=1)
        ax2.set_ylabel("Temperature (°C)", fontsize=10)
        ax2.set_title("Temperature", fontsize=12)
        ax2.grid(True, alpha=0.3)

        # Plot Humidity
        ax3.plot(df.index, df["humidity"], "g-", linewidth=1)
        ax3.set_ylabel("Humidity (%)", fontsize=10)
        ax3.set_xlabel("Time", fontsize=10)
        ax3.set_title("Humidity", fontsize=12)
        ax3.grid(True, alpha=0.3)

        # Rotate x-axis labels
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()

        # Convert to base64 string
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format="png", dpi=100, bbox_inches="tight")
        img_buffer.seek(0)
        img_base64 = base64.b64encode(img_buffer.read()).decode("utf-8")
        plt.close()

        return img_base64

    except Exception as e:
        print(f"Error generating CO2 plot: {e}")
        import traceback

        traceback.print_exc()
        return None


def get_box_video_url(
    api_key: str,
    base_id: str,
    cage_id: str,
    box_id: str,
    start_date: Optional[str],
    aws_access_key: str,
    aws_secret_key: str,
    s3_bucket: str = "rp-raw-olio",
    timestamp_override: Optional[str] = None,
    experiment_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get presigned S3 URL for the most recent usbcam video for a cage.
    If start_date is provided, uses ONLY the experiment_id for that date.
    If start_date is None, falls back to the prior behavior (today, then yesterday).

    Args:
        api_key: Airtable API key
        base_id: Airtable base ID
        cage_id: Cage ID (e.g., 'c0000750')
        box_id: Box ID (e.g., 'b0000041')
        aws_access_key: AWS access key for S3
        aws_secret_key: AWS secret key for S3
        s3_bucket: S3 bucket name (default: 'rp-raw-olio')

    Returns:
        {
            'success': bool,
            'video_url': str (if success),
            'error': str (if not success),
            'cage_id': str (if success),
            'experiment_id': str (if success),
            'co2_plot': str (if success and CO2 data available) - base64 PNG
        }
    """
    try:
        date_str: Optional[str] = (
            start_date.strip()
            if isinstance(start_date, str) and start_date.strip()
            else None
        )
        if date_str is not None:
            _parse_yyyy_mm_dd(date_str)

        # Initialize S3 client
        s3_client = boto3.client(
            "s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
        )

        # Resolve start_date (YYYY-MM-DD) and "now" in PST for UI defaults.
        now_pst = datetime.now(ZoneInfo("America/Los_Angeles"))
        today_start_date_str = now_pst.date().strftime("%Y-%m-%d")

        # Canonical behavior:
        # - If experiment_id is provided: derive start_date
        #   from Airtable and use the experiment window.
        # - Else: use provided start_date (or default PST
        #   today) and resolve experiment_id for that date.
        exp_id = (experiment_id_override or "").strip()
        start_date_effective = date_str or today_start_date_str
        if exp_id:
            sd, err = get_start_date_for_experiment_id(api_key, base_id, exp_id)
            if err or not sd:
                return {
                    "success": False,
                    "error": err
                    or f"Could not resolve start_date for experiment_id {exp_id}",
                }
            start_date_effective = sd
        else:
            exp_id = _get_experiment_id_for_box_on_date(
                api_key, base_id, box_id, start_date_effective
            )
            if not exp_id:
                return {
                    "success": False,
                    "error": (
                        f"No experiment_id found for"
                        f" box {box_id}"
                        f" on {start_date_effective}"
                    ),
                }

        videos = _list_usbcam_videos_for_experiment_window(
            s3_client=s3_client,
            experiment_id=exp_id,
            box_id=box_id,
            cage_id=cage_id,
            s3_bucket=s3_bucket,
            start_date_str=start_date_effective,
        )

        if not videos:
            return {
                "success": False,
                "error": "No video available for selected experiment/start_date window",
            }

        available = [
            {
                "timestamp": v["timestamp_str"],
                "label": _format_timestamp_label_pst(v["timestamp_str"]),
            }
            for v in videos
        ]

        selected_ts = (timestamp_override or "").strip()
        selected_video = None
        if selected_ts:
            for v in videos:
                if v["timestamp_str"] == selected_ts:
                    selected_video = v
                    break
            if not selected_video:
                return {
                    "success": False,
                    "error": (
                        f"Requested timestamp"
                        f" {selected_ts} not found for"
                        f" experiment {exp_id} in"
                        f" start_date window"
                        f" {start_date_effective}."
                    ),
                }
        else:
            selected_video = videos[0]

        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_bucket, "Key": selected_video["key"]},
            ExpiresIn=3600,
        )

        # Look for corresponding CO2 CSV file for this timestamp.
        # (We can reuse the existing helper by calling
        # _try_find_recent_video_in_hour, but it's cheaper
        # to scan once.)
        co2_csv_key = None
        try:
            # This is consistent with the naming used elsewhere.
            ts_str = selected_video["timestamp_str"]
            co2_csv_key = (
                f"{exp_id}/{box_id}/{cage_id}/"
                f"{exp_id}_{ts_str}"
                "_co2-sensor-scd40-0.csv.gz"
            )
            # Verify existence quickly (head_object is one
            # request). If it fails, we just skip plotting.
            s3_client.head_object(Bucket=s3_bucket, Key=co2_csv_key)
        except Exception:
            co2_csv_key = None

        result: Dict[str, Any] = {
            "success": True,
            "video_url": presigned_url,
            "cage_id": cage_id,
            "experiment_id": exp_id,
            "timestamp": selected_video["timestamp_str"],
            "start_date": start_date_effective,
            "available_timestamps": available,
        }

        if co2_csv_key:
            co2_plot = _generate_co2_plot(s3_client, co2_csv_key, s3_bucket)
            if co2_plot:
                result["co2_plot"] = co2_plot

        return result

    except Exception as e:
        print(f"Error getting box video URL: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": "Error loading video"}


def _get_experiment_ids_for_box(
    api_key: str, base_id: str, box_id: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get both today's and yesterday's experiment_ids for a box.

    Returns:
        (today_exp_id, yesterday_exp_id) - both may be None
    """
    try:
        table = Api(api_key).table(base_id, "experiment_planner")

        # Get today and yesterday dates
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime("%Y-%m-%d")
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        # Build lightweight formula using Airtable's IS_SAME
        # function for date comparison
        # IS_SAME compares dates properly regardless of time component
        formula = (
            f"AND({{box_id}} = '{box_id}',"
            f" OR(IS_SAME({{start_date}},"
            f" '{today_str}', 'day'),"
            f" IS_SAME({{start_date}},"
            f" '{yesterday_str}', 'day')))"
        )

        # Only fetch minimal fields needed
        fields = ["experiment_id", "box_id", "start_date"]

        print(
            f"Looking for experiment for box {box_id} on {today_str} or {yesterday_str}"
        )
        print(f"Formula: {formula}")
        records = table.all(formula=formula, fields=fields)
        print(f"Found {len(records)} record(s)")
        print(records)

        if not records:
            print(f"No experiments found for box {box_id} in the last 2 days")
            return None, None

        today_exp_id = None
        yesterday_exp_id = None

        # Parse both today's and yesterday's experiments
        for record in records:
            fields_data = record.get("fields", {})
            start_date = fields_data.get("start_date")

            # Handle if it's a list (lookup field)
            if isinstance(start_date, list) and start_date:
                start_date = start_date[0]

            exp_id = fields_data.get("experiment_id")
            if isinstance(exp_id, list) and exp_id:
                exp_id = exp_id[0]

            if start_date == today_str and exp_id:
                today_exp_id = str(exp_id)
                print(f"Found today's experiment_id: {today_exp_id}")
            elif start_date == yesterday_str and exp_id:
                yesterday_exp_id = str(exp_id)
                print(f"Found yesterday's experiment_id: {yesterday_exp_id}")

        return today_exp_id, yesterday_exp_id

    except Exception as e:
        print(f"Error getting experiment_ids for box {box_id}: {e}")
        import traceback

        traceback.print_exc()
        return None, None


def _get_experiment_id_for_box_on_date(
    api_key: str, base_id: str, box_id: str, date_str: str
) -> Optional[str]:
    """
    Get experiment_id for a box on a specific start_date (YYYY-MM-DD).
    """
    try:
        _parse_yyyy_mm_dd(date_str)
        table = Api(api_key).table(base_id, "experiment_planner")
        formula = (
            f"AND({{box_id}} = '{box_id}',"
            f" IS_SAME({{start_date}},"
            f" '{date_str}', 'day'))"
        )
        fields = ["experiment_id", "start_date"]
        records = table.all(formula=formula, fields=fields)
        if not records:
            return None
        # Prefer the first record; if multiple exist,
        # we intentionally don't guess beyond that.
        fields_data = records[0].get("fields", {}) or {}
        exp_id = fields_data.get("experiment_id")
        if isinstance(exp_id, list) and exp_id:
            exp_id = exp_id[0]
        return str(exp_id) if exp_id else None
    except Exception as e:
        print(f"Error getting experiment_id for box {box_id} on {date_str}: {e}")
        import traceback

        traceback.print_exc()
        return None


def _get_experiment_id_for_box(
    api_key: str, base_id: str, box_id: str
) -> Optional[str]:
    """
    Get experiment_id for a box with a lightweight Airtable query.
    Fetches only today and yesterday's experiments for this specific box.
    First tries today's date, then tries yesterday's experiment_id.

    Returns experiment_id string like 'e0000479' or None if not found.

    Deprecated: Use _get_experiment_ids_for_box instead.
    """
    today_exp_id, yesterday_exp_id = _get_experiment_ids_for_box(
        api_key, base_id, box_id
    )
    return today_exp_id or yesterday_exp_id


_CART_EVENT_METADATA_COLUMNS: List[str] = [
    "experiment_id",
    "box_id",
    "cage_id",
    "cart",
    "start_frame_id",
    "end_frame_id",
    "start_timestamp",
    "end_timestamp",
    "video_s3_path",
    "timestamp_s3_path",
    "device_uuid",
    "camera_type",
    "video_s3_path_lowres",
    "measurement_timing",
    "syringe_qr_frame_id_1",
    "syringe_qr_timestamp_1",
    "syringe_qr_frame_id_2",
    "syringe_qr_timestamp_2",
    "manipulation_id_inputted",
    "correct_incorrect",
]


def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    s = (s3_uri or "").strip()
    if not s.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI (expected s3://...): {s3_uri!r}")
    parsed = urlparse(s)
    bucket = parsed.netloc
    key = (parsed.path or "").lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI (missing bucket or key): {s3_uri!r}")
    return bucket, key


_CART_EVENT_CACHE_INITIALIZED = False


def _get_cart_event_cache_dir() -> Path:
    # Keep it inside the repo by default (easy to inspect, sandbox-friendly).
    # Cleared once per app process start (see _init_cart_event_cache()).
    base = os.getenv("CART_EVENT_CACHE_DIR")
    if base:
        return Path(base).expanduser()
    return Path(__file__).resolve().parents[2] / ".cache" / "cart_event"


def _init_cart_event_cache() -> Path:
    """
    Initialize cache directory structure and clear it once per process startup.
    """
    global _CART_EVENT_CACHE_INITIALIZED
    cache_dir = _get_cart_event_cache_dir()
    if _CART_EVENT_CACHE_INITIALIZED:
        return cache_dir

    # Clear cache on startup (process). Fail loud if we can't, because we'd otherwise
    # end up in a confusing half-cached state.
    try:
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        (cache_dir / "sources").mkdir(parents=True, exist_ok=True)
        (cache_dir / "clips").mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise RuntimeError(
            f"Failed to initialize cart event cache dir at {cache_dir}: {e}"
        )

    _CART_EVENT_CACHE_INITIALIZED = True
    return cache_dir


def _safe_cache_name(bucket: str, key: str) -> str:
    # Deterministic, filesystem-safe name while still reasonably debuggable.
    h = hashlib.sha256(f"{bucket}/{key}".encode("utf-8")).hexdigest()[:16]
    base = key.split("/")[-1] or "video"
    base = re.sub(r"[^a-zA-Z0-9_.-]+", "_", base)
    return f"{base}__{h}"


def _download_source_video_to_cache(s3_client, s3_uri: str) -> Path:
    bucket, key = _parse_s3_uri(s3_uri)
    cache_dir = _init_cart_event_cache()
    name = _safe_cache_name(bucket, key)
    # Preserve extension if present
    ext = ""
    if "." in key.split("/")[-1]:
        ext = "." + key.split("/")[-1].split(".")[-1]
    dst = cache_dir / "sources" / f"{name}{ext}"
    if dst.exists() and dst.stat().st_size > 0:
        return dst

    tmp = dst.with_suffix(dst.suffix + ".part")
    try:
        # Ensure parent exists
        tmp.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket, key, str(tmp))
        if not tmp.exists() or tmp.stat().st_size <= 0:
            raise RuntimeError("Downloaded file is empty.")
        os.replace(str(tmp), str(dst))
        return dst
    except Exception as e:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise RuntimeError(f"Failed to download source video {s3_uri} to cache: {e}")


def _s3_select_cart_event_rows(
    s3_client,
    metadata_bucket: str,
    metadata_key: str,
    experiment_id: str,
    box_id: str,
    cage_id: str,
) -> List[Dict[str, Any]]:
    """
    Query cart_event_metadata.csv using S3 Select and return matching rows as dicts.

    We intentionally rely on S3 Select to avoid downloading a potentially large CSV.
    """
    # Basic SQL escaping: IDs shouldn't contain quotes, but fail loud if they do.
    for name, v in [
        ("experiment_id", experiment_id),
        ("box_id", box_id),
        ("cage_id", cage_id),
    ]:
        if "'" in (v or ""):
            raise ValueError(
                f"Invalid {name}: contains a single"
                " quote which is not supported"
                " for S3 Select filters."
            )

    expr = (
        "SELECT * FROM S3Object s "
        f"WHERE s.experiment_id = '{experiment_id}' "
        f"AND s.box_id = '{box_id}' "
        f"AND s.cage_id = '{cage_id}' "
        "AND (s.camera_type = 'cart_box' OR s.camera_type = 'cart_injection')"
    )

    try:
        resp = s3_client.select_object_content(
            Bucket=metadata_bucket,
            Key=metadata_key,
            ExpressionType="SQL",
            Expression=expr,
            InputSerialization={
                "CSV": {
                    "FileHeaderInfo": "USE",
                    "FieldDelimiter": ",",
                    "RecordDelimiter": "\n",
                },
                "CompressionType": "NONE",
            },
            OutputSerialization={"CSV": {}},
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to query s3://{metadata_bucket}/{metadata_key} via S3 Select: {e}"
        )

    raw = b""
    for event in resp.get("Payload", []):
        if "Records" in event:
            raw += event["Records"].get("Payload", b"")

    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return []

    rows: List[Dict[str, Any]] = []
    reader = csv.reader(io.StringIO(text))
    for values in reader:
        if not values:
            continue
        if len(values) != len(_CART_EVENT_METADATA_COLUMNS):
            raise ValueError(
                f"Unexpected cart_event_metadata column"
                f" count {len(values)}"
                f" (expected"
                f" {len(_CART_EVENT_METADATA_COLUMNS)})."
                f" Row starts with: {values[:4]}"
            )
        rows.append({k: v for k, v in zip(_CART_EVENT_METADATA_COLUMNS, values)})
    return rows


def _to_float_strict(name: str, v: Any) -> float:
    s = str(v).strip() if v is not None else ""
    if s == "":
        raise ValueError(f"Missing required numeric field {name}.")
    try:
        return float(s)
    except Exception:
        raise ValueError(f"Invalid numeric field {name}: {v!r}")


def _compute_clip_times_from_row(
    *,
    row: Dict[str, Any],
    chosen_start_frame_id: float,
) -> Dict[str, Any]:
    start_frame_id = _to_float_strict("start_frame_id", row.get("start_frame_id"))
    end_frame_id = _to_float_strict("end_frame_id", row.get("end_frame_id"))
    start_ts_ms = _to_float_strict("start_timestamp", row.get("start_timestamp"))
    end_ts_ms = _to_float_strict("end_timestamp", row.get("end_timestamp"))

    if end_frame_id <= start_frame_id:
        raise ValueError(
            f"Invalid frame range: end_frame_id"
            f" ({end_frame_id}) <= start_frame_id"
            f" ({start_frame_id})."
        )

    duration_s = (end_ts_ms - start_ts_ms) / 1000.0
    if duration_s <= 0:
        raise ValueError(
            f"Invalid timestamp range: end_timestamp"
            f" ({end_ts_ms}) <= start_timestamp"
            f" ({start_ts_ms})."
        )

    fps = (end_frame_id - start_frame_id) / duration_s
    if not (fps > 0):
        raise ValueError(f"Computed non-positive fps: {fps}.")

    if chosen_start_frame_id < start_frame_id or chosen_start_frame_id > end_frame_id:
        raise ValueError(
            f"Chosen start frame {chosen_start_frame_id} is outside video frame range "
            f"[{start_frame_id}, {end_frame_id}]."
        )

    clip_start_seconds = (chosen_start_frame_id - start_frame_id) / fps
    clip_end_seconds = (end_frame_id - start_frame_id) / fps

    return {
        "fps": fps,
        "start_frame_id": start_frame_id,
        "end_frame_id": end_frame_id,
        "clip_start_seconds": clip_start_seconds,
        "clip_end_seconds": clip_end_seconds,
    }


def get_cart_event_videos(
    *,
    api_key: str,
    base_id: str,
    cage_id: str,
    box_id: str,
    start_date: str,
    experiment_id_override: Optional[str] = None,
    aws_access_key: str,
    aws_secret_key: str,
    metadata_bucket: str = "rodent-party",
    metadata_key: str = "internal/metadata/cart_event_metadata.csv",
) -> Dict[str, Any]:
    """
    Return presigned cart_box and cart_injection video URLs (full-res) and clip timing
    computed from frame IDs per cart_event_metadata.csv.
    """
    cage_id = str(cage_id or "").strip()
    box_id = str(box_id or "").strip()
    date_str = str(start_date or "").strip()
    if not cage_id or not box_id or not date_str:
        return {
            "success": False,
            "error": "Missing cage_id, box_id, or date parameter",
            "status_code": 400,
        }

    try:
        _parse_yyyy_mm_dd(date_str)
    except Exception as e:
        return {"success": False, "error": str(e), "status_code": 400}

    if not api_key or not base_id:
        return {
            "success": False,
            "error": "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID",
            "status_code": 500,
        }

    try:
        experiment_id = (
            experiment_id_override or ""
        ).strip() or _get_experiment_id_for_box_on_date(
            api_key, base_id, box_id, date_str
        )
        if not experiment_id:
            return {
                "success": False,
                "error": f"No experiment_id found for box {box_id} on {date_str}",
                "status_code": 404,
            }
    except Exception as e:
        return {
            "success": False,
            "error": (
                f"Error resolving experiment_id for box {box_id} on {date_str}: {e}"
            ),
            "status_code": 500,
        }

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
    except Exception as e:
        return {
            "success": False,
            "error": f"Error initializing S3 client: {e}",
            "status_code": 500,
        }

    try:
        rows = _s3_select_cart_event_rows(
            s3_client=s3_client,
            metadata_bucket=metadata_bucket,
            metadata_key=metadata_key,
            experiment_id=str(experiment_id),
            box_id=box_id,
            cage_id=cage_id,
        )
    except Exception as e:
        return {
            "success": False,
            "error": (
                f"Error querying cart event metadata"
                f" for experiment {experiment_id},"
                f" box {box_id},"
                f" cage {cage_id}: {e}"
            ),
            "status_code": 500,
        }

    def pick_single(
        rows_in: List[Dict[str, Any]], label: str
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        """
        Select a single row for a clip type.

        Requirement: some (experiment_id, box_id, cage_id,
        camera_type) combinations have multiple rows.
        We choose the *first* row (in CSV/S3 Select return
        order) that has a non-empty syringe_qr_frame_id_1.
        If none have syringe_qr_frame_id_1, we fail loud.
        """
        if not rows_in:
            return (
                None,
                f"No {label} row found in"
                f" cart_event_metadata for"
                f" experiment {experiment_id},"
                f" box {box_id},"
                f" cage {cage_id}.",
                404,
            )

        for r in rows_in:
            if str(r.get("syringe_qr_frame_id_1") or "").strip() != "":
                return r, None, 200

        return (
            None,
            f"No {label} row with"
            f" syringe_qr_frame_id_1 found in"
            f" cart_event_metadata for"
            f" experiment {experiment_id},"
            f" box {box_id},"
            f" cage {cage_id}.",
            404,
        )

    cart_box_rows = [
        r for r in rows if (r.get("camera_type") or "").strip() == "cart_box"
    ]
    cart_injection_rows = [
        r
        for r in rows
        if (r.get("camera_type") or "").strip() == "cart_injection"
        and (r.get("correct_incorrect") or "").strip().lower() == "correct"
    ]

    cart_box_row, err, code = pick_single(cart_box_rows, "cart_box")
    if err:
        return {"success": False, "error": err, "status_code": code}
    inj_row, err, code = pick_single(cart_injection_rows, "cart_injection(correct)")
    if err:
        return {"success": False, "error": err, "status_code": code}

    try:
        # cart_box clip uses start_frame_id -> end_frame_id
        cart_box_times = _compute_clip_times_from_row(
            row=cart_box_row,
            chosen_start_frame_id=_to_float_strict(
                "start_frame_id", cart_box_row.get("start_frame_id")
            ),
        )

        # cart_injection clip uses syringe_qr_frame_id_1 -> end_frame_id
        syringe_start = _to_float_strict(
            "syringe_qr_frame_id_1", inj_row.get("syringe_qr_frame_id_1")
        )
        cart_injection_times = _compute_clip_times_from_row(
            row=inj_row,
            chosen_start_frame_id=syringe_start,
        )

        cart_box_bucket, cart_box_key = _parse_s3_uri(cart_box_row.get("video_s3_path"))
        inj_bucket, inj_key = _parse_s3_uri(inj_row.get("video_s3_path"))

        cart_box_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": cart_box_bucket, "Key": cart_box_key},
            ExpiresIn=3600,
        )
        inj_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": inj_bucket, "Key": inj_key},
            ExpiresIn=3600,
        )

        return {
            "success": True,
            "experiment_id": str(experiment_id),
            "box_id": box_id,
            "cage_id": cage_id,
            "cart_box": {
                "camera_type": "cart_box",
                "video_s3_path": cart_box_row.get("video_s3_path"),
                "video_url": cart_box_url,
                **cart_box_times,
            },
            "cart_injection": {
                "camera_type": "cart_injection",
                "video_s3_path": inj_row.get("video_s3_path"),
                "video_url": inj_url,
                "syringe_qr_frame_id_1": syringe_start,
                **cart_injection_times,
            },
        }
    except Exception as e:
        return {
            "success": False,
            "error": (
                f"Error computing cart clip"
                f" times/presigned URLs for"
                f" experiment {experiment_id},"
                f" box {box_id},"
                f" cage {cage_id}: {e}"
            ),
            "status_code": 500,
        }


def generate_cart_event_clip_file(
    *,
    api_key: str,
    base_id: str,
    cage_id: str,
    box_id: str,
    start_date: str,
    kind: str,
    experiment_id_override: Optional[str] = None,
    aws_access_key: str,
    aws_secret_key: str,
    metadata_bucket: str = "rodent-party",
    metadata_key: str = "internal/metadata/cart_event_metadata.csv",
) -> Dict[str, Any]:
    """
    Generate a trimmed MP4 clip file for either cart_box
    or cart_injection and return a temp path.

    "Good enough" trimming: time-based using ffmpeg -ss/-t and stream copy (-c copy).
    This is not guaranteed frame-accurate (keyframe
    limitations), but avoids loading full-hour videos
    in the browser.
    """
    kind_norm = (kind or "").strip().lower()
    if kind_norm not in ("cart_box", "cart_injection"):
        return {
            "success": False,
            "error": f"Invalid kind '{kind}'. Expected 'cart_box' or 'cart_injection'.",
            "status_code": 400,
        }

    meta = get_cart_event_videos(
        api_key=api_key,
        base_id=base_id,
        cage_id=cage_id,
        box_id=box_id,
        start_date=start_date,
        experiment_id_override=experiment_id_override,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        metadata_bucket=metadata_bucket,
        metadata_key=metadata_key,
    )
    if not meta.get("success"):
        return meta

    clip = meta.get(kind_norm)
    if not isinstance(clip, dict) or not clip.get("video_url"):
        return {
            "success": False,
            "error": (
                f"Missing {kind_norm} clip data for"
                f" experiment"
                f" {meta.get('experiment_id')},"
                f" box {box_id},"
                f" cage {cage_id}."
            ),
            "status_code": 500,
        }

    try:
        start_s = float(clip.get("clip_start_seconds"))
        end_s = float(clip.get("clip_end_seconds"))
    except Exception:
        return {
            "success": False,
            "error": (
                f"Invalid clip timing for"
                f" {kind_norm} (start/end seconds"
                " missing or non-numeric)."
            ),
            "status_code": 500,
        }

    if not (start_s >= 0 and end_s > start_s):
        return {
            "success": False,
            "error": (
                f"Invalid clip timing for {kind_norm}: start={start_s}, end={end_s}."
            ),
            "status_code": 500,
        }

    video_s3_path = str(clip.get("video_s3_path")).strip()
    if not video_s3_path:
        return {
            "success": False,
            "error": f"Missing video_s3_path for {kind_norm}.",
            "status_code": 500,
        }

    # Ensure cache dir exists/cleared for this process.
    cache_dir = _init_cart_event_cache()

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
    except Exception as e:
        return {
            "success": False,
            "error": f"Error initializing S3 client: {e}",
            "status_code": 500,
        }

    # Download the source video locally (we need random frame access for OpenCV).
    try:
        src_path = _download_source_video_to_cache(s3_client, video_s3_path)
    except Exception as e:
        return {"success": False, "error": str(e), "status_code": 500}

    # Cache clip outputs deterministically.
    # IMPORTANT: Frame IDs in cart_event_metadata.csv are
    # treated as absolute frame indices
    # into the video file. For accurate clipping, do NOT offset by start_frame_id.
    try:
        start_frame_id = int(float(str(clip.get("start_frame_id")).strip()))
        end_frame_id = int(float(str(clip.get("end_frame_id")).strip()))
    except Exception:
        return {
            "success": False,
            "error": f"Missing/invalid start_frame_id or end_frame_id for {kind_norm}.",
            "status_code": 500,
        }

    if kind_norm == "cart_injection":
        try:
            clip_start_frame = int(
                float(str(clip.get("syringe_qr_frame_id_1")).strip())
            )
        except Exception:
            return {
                "success": False,
                "error": "Missing/invalid syringe_qr_frame_id_1 for cart_injection.",
                "status_code": 500,
            }
    else:
        clip_start_frame = start_frame_id

    clip_end_frame = end_frame_id
    if clip_start_frame < 0 or clip_end_frame < clip_start_frame:
        return {
            "success": False,
            "error": (
                f"Invalid frame range for"
                f" {kind_norm}:"
                f" start={clip_start_frame},"
                f" end={clip_end_frame}."
            ),
            "status_code": 500,
        }

    clip_key = (
        f"{meta.get('experiment_id')}|{box_id}"
        f"|{cage_id}|{kind_norm}"
        f"|{video_s3_path}"
        f"|{clip_start_frame}|{clip_end_frame}"
    )
    clip_hash = hashlib.sha256(clip_key.encode("utf-8")).hexdigest()[:16]
    clip_path = cache_dir / "clips" / f"{kind_norm}_{clip_hash}.mp4"
    if clip_path.exists() and clip_path.stat().st_size > 0:
        return {
            "success": True,
            "file_path": str(clip_path),
            "experiment_id": meta.get("experiment_id"),
            "box_id": box_id,
            "cage_id": cage_id,
            "kind": kind_norm,
            "clip_start_seconds": start_s,
            "clip_end_seconds": end_s,
            "cached": True,
        }

    # Write to a temp file then atomically move into place (race-safe).
    try:
        tmp = tempfile.NamedTemporaryFile(
            prefix=f"{kind_norm}_",
            suffix=".mp4",
            dir=str(cache_dir / "clips"),
            delete=False,
        )
        tmp_path = Path(tmp.name)
        tmp.close()
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to create temp file for clip: {e}",
            "status_code": 500,
        }

    try:
        cap = cv2.VideoCapture(str(src_path))
        if not cap.isOpened():
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": f"Could not open video with OpenCV: {video_s3_path}",
                "status_code": 500,
            }

        # Seek to the *absolute* frame index
        cap.set(cv2.CAP_PROP_POS_FRAMES, clip_start_frame)

        # Determine FPS for output
        fps_out = cap.get(cv2.CAP_PROP_FPS)
        if not fps_out or fps_out <= 0:
            try:
                fps_out = float(clip.get("fps") or 0)
            except Exception:
                fps_out = 0
        if not fps_out or fps_out <= 0:
            cap.release()
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": f"Could not determine FPS for {kind_norm} clip.",
                "status_code": 500,
            }

        # Read first frame to get dimensions
        ret, first_frame = cap.read()
        if not ret or first_frame is None:
            cap.release()
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": (
                    f"Could not read start frame"
                    f" {clip_start_frame}"
                    f" for {kind_norm} clip."
                ),
                "status_code": 500,
            }

        # Pure OpenCV clip writing (frame-accurate).
        # We prefer H.264 if OpenCV was built with it; otherwise fall back to mp4v.
        h, w = first_frame.shape[:2]
        if h <= 0 or w <= 0:
            cap.release()
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": f"Invalid frame dimensions for {kind_norm} clip: {w}x{h}.",
                "status_code": 500,
            }

        fourcc_candidates = [
            cv2.VideoWriter_fourcc(
                *"avc1"
            ),  # H.264 (if available in this OpenCV build)
            cv2.VideoWriter_fourcc(*"mp4v"),  # MPEG-4 Part 2 (widely available)
        ]

        out = None
        for fourcc in fourcc_candidates:
            writer = cv2.VideoWriter(
                str(tmp_path), fourcc, float(fps_out), (int(w), int(h))
            )
            if writer.isOpened():
                out = writer
                break
            try:
                writer.release()
            except Exception:
                pass

        if out is None or not out.isOpened():
            cap.release()
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": (
                    f"Could not open an MP4"
                    f" VideoWriter for {kind_norm}"
                    " clip (tried avc1, mp4v)."
                ),
                "status_code": 500,
            }

        # Write frames from clip_start_frame..clip_end_frame (inclusive)
        try:
            out.write(first_frame)
            current_frame = clip_start_frame + 1
            while current_frame <= clip_end_frame:
                ret, frame = cap.read()
                if not ret or frame is None:
                    break
                if frame.shape[0] != h or frame.shape[1] != w:
                    raise ValueError(
                        f"Frame size changed mid-stream"
                        f" ({frame.shape[1]}"
                        f"x{frame.shape[0]}),"
                        " refusing to encode."
                    )
                out.write(frame)
                current_frame += 1
        finally:
            try:
                out.release()
            except Exception:
                pass
            try:
                cap.release()
            except Exception:
                pass

        if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": f"OpenCV produced an empty {kind_norm} clip.",
                "status_code": 500,
            }

        # Atomically promote temp to cached clip path (or if
        # another request won the race, prefer the cached
        # file).
        try:
            if clip_path.exists() and clip_path.stat().st_size > 0:
                tmp_path.unlink(missing_ok=True)
            else:
                os.replace(str(tmp_path), str(clip_path))
        except Exception as e:
            # Fail loud: we don't want to return a path that doesn't exist.
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return {
                "success": False,
                "error": f"Failed to move generated clip into cache: {e}",
                "status_code": 500,
            }

        if not clip_path.exists() or clip_path.stat().st_size <= 0:
            return {
                "success": False,
                "error": (
                    "Clip cache write succeeded but"
                    " file is missing/empty"
                    f" at {clip_path}"
                ),
                "status_code": 500,
            }

        return {
            "success": True,
            "file_path": str(clip_path),
            "experiment_id": meta.get("experiment_id"),
            "box_id": box_id,
            "cage_id": cage_id,
            "kind": kind_norm,
            "clip_start_seconds": start_s,
            "clip_end_seconds": end_s,
            "cached": False,
        }
    except Exception as e:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return {
            "success": False,
            "error": f"Error generating {kind_norm} clip: {e}",
            "status_code": 500,
        }
