"""Box room API endpoints."""

from __future__ import annotations

import concurrent.futures
import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from ..config import settings
from ..services.box_room_service import (
    fetch_box_room_data,
    fetch_cages_without_box_data,
    get_box_video_url,
    get_cart_event_videos,
    get_experiment_ids_for_start_date,
    get_start_date_for_experiment_id,
)
from ..services.box_room_service import (
    get_box_flagged_issues_history as _get_box_history,
)

logger = logging.getLogger(__name__)
router = APIRouter()

API_KEY = settings.airtable_api_key
BASE_ID = settings.airtable_base_id


def _build_two_row_bank(start: int, end: int) -> dict:
    count = end - start + 1
    half = count // 2
    bottom = list(range(start, start + half))
    top = list(range(start + half, end + 1))
    return {"top": top, "bottom": bottom}


def _build_two_row_bank_reversed(start: int, end: int) -> dict:
    count = end - start + 1
    half = count // 2
    top = list(range(start, start + half))
    bottom = list(range(start + half, end + 1))
    return {"top": top[::-1], "bottom": bottom[::-1]}


def _build_two_column_bank(start: int, end: int) -> dict:
    count = end - start + 1
    half = count // 2
    left = list(range(start, start + half))
    right = list(range(start + half, end + 1))
    return {"left": left, "right": right}


def _build_two_column_bank_reversed(start: int, end: int) -> dict:
    count = end - start + 1
    half = count // 2
    left = list(range(start, start + half))
    right = list(range(start + half, end + 1))
    return {"left": right[::-1], "right": left[::-1]}


BANKS = {
    "bank_9_24": _build_two_row_bank(9, 24),
    "bank_25_40": _build_two_row_bank(25, 40),
    "bank_41_48": _build_two_column_bank_reversed(41, 48),
    "bank_49_56": _build_two_column_bank(49, 56),
    "bank_57_64": _build_two_column_bank(57, 64),
    "bank_65_72": _build_two_row_bank_reversed(65, 72),
    "bank_73_80": _build_two_row_bank_reversed(73, 80),
    "bank_81_88": _build_two_column_bank_reversed(81, 88),
}


@router.get("")
def get_box_room_data(
    start_date: Optional[str] = Query(None),
    experiment_id: Optional[str] = Query(None),
) -> dict:
    """Get box room layout data with cage assignments and overlays."""
    try:
        selected_date = (start_date or "").strip()
        experiment_id_filter = (experiment_id or "").strip()

        if selected_date:
            try:
                datetime.strptime(selected_date, "%Y-%m-%d")
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid start_date '{selected_date}'. Expected YYYY-MM-DD."
                    ),
                )
        else:
            selected_date = (
                datetime.now(ZoneInfo("America/Los_Angeles"))
                .date()
                .strftime("%Y-%m-%d")
            )

        today_pst_str = (
            datetime.now(ZoneInfo("America/Los_Angeles")).date().strftime("%Y-%m-%d")
        )

        # Canonicalize: resolve experiment_id <-> start_date
        if experiment_id_filter and not start_date:
            sd, err = get_start_date_for_experiment_id(
                API_KEY, BASE_ID, experiment_id_filter
            )
            if err or not sd:
                raise HTTPException(
                    status_code=400,
                    detail=err or "Could not resolve start_date for experiment_id",
                )
            selected_date = sd

        if start_date and not experiment_id_filter:
            exp_ids, exp_err = get_experiment_ids_for_start_date(
                API_KEY, BASE_ID, start_date
            )
            if not exp_err and exp_ids and len(exp_ids) == 1:
                experiment_id_filter = exp_ids[0]

        # Parallel Airtable reads
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            fut_boxes = ex.submit(
                fetch_box_room_data,
                API_KEY,
                BASE_ID,
                selected_date,
                experiment_id_filter or None,
            )
            fut_cages = ex.submit(fetch_cages_without_box_data, API_KEY, BASE_ID, True)
            fut_today_exp = ex.submit(
                get_experiment_ids_for_start_date,
                API_KEY,
                BASE_ID,
                today_pst_str,
            )
            if selected_date == today_pst_str:
                fut_selected_exp = fut_today_exp
            else:
                fut_selected_exp = ex.submit(
                    get_experiment_ids_for_start_date,
                    API_KEY,
                    BASE_ID,
                    selected_date,
                )

            boxes_by_number, boxes_with_issues, overlay_errors = fut_boxes.result()
            cages_data, cages_with_issues = fut_cages.result()
            today_exp_ids, today_exp_err = fut_today_exp.result()
            today_experiment_id = today_exp_ids[0] if today_exp_ids else ""
            selected_exp_ids, selected_exp_err = fut_selected_exp.result()
            selected_experiment_id = selected_exp_ids[0] if selected_exp_ids else ""

        # Serialize box data (convert int keys to strings for JSON)
        serialized_boxes = {str(k): v for k, v in boxes_by_number.items()}

        return {
            "boxes_by_number": serialized_boxes,
            "boxes_with_issues": boxes_with_issues,
            "overlay_errors": overlay_errors,
            "cages_data": cages_data,
            "cages_with_issues": cages_with_issues,
            "banks": BANKS,
            "selected_date": selected_date,
            "today_pst_date": today_pst_str,
            "today_experiment_id": today_experiment_id,
            "today_experiment_error": today_exp_err,
            "selected_experiment_id": selected_experiment_id,
            "selected_experiment_error": selected_exp_err,
            "experiment_id_filter": experiment_id_filter,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading box room: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flagged-issues/{box_number}")
def get_box_flagged_issues(
    box_number: int,
    start_date: Optional[str] = Query(None),
    experiment_id: Optional[str] = Query(None),
) -> list:
    """Get flagged issues history for a specific box."""
    try:
        date_str = (start_date or "").strip() or None
        exp_id = (experiment_id or "").strip() or None
        return _get_box_history(box_number, date_str, exp_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cage-flagged-issues/{cage_id}")
def get_cage_flagged_issues(cage_id: str) -> list:
    """Get flagged issues history for a specific cage."""
    try:
        from ..services.box_room_service import (
            get_cage_flagged_issues_history,
        )

        return get_cage_flagged_issues_history(cage_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/video")
def get_box_video_endpoint(
    cage_id: str = Query(...),
    box_id: str = Query(...),
    start_date: Optional[str] = Query(None),
    timestamp: Optional[str] = Query(None),
    experiment_id: Optional[str] = Query(None),
) -> dict:
    """Get presigned S3 URL for box video."""
    if not cage_id or cage_id == "undefined":
        raise HTTPException(
            status_code=400,
            detail="Missing or invalid cage_id parameter",
        )
    date_str = (start_date or "").strip()
    if date_str:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid start_date '{date_str}'. Expected YYYY-MM-DD.",
            )
    else:
        date_str = datetime.now().date().strftime("%Y-%m-%d")

    result = get_box_video_url(
        api_key=API_KEY,
        base_id=BASE_ID,
        cage_id=cage_id,
        box_id=box_id,
        start_date=date_str,
        s3_bucket="rp-raw-olio",
        timestamp_override=(timestamp or "").strip() or None,
        experiment_id_override=(experiment_id or "").strip() or None,
    )

    if result["success"]:
        return result

    msg = str(result.get("error") or "")
    status = 404 if ("No video available" in msg or "not found" in msg) else 500
    raise HTTPException(status_code=status, detail=msg)


@router.get("/cart-videos")
def get_cart_videos(
    cage_id: str = Query(...),
    box_id: str = Query(...),
    start_date: str = Query(...),
    experiment_id: Optional[str] = Query(None),
) -> dict:
    """Get cart event videos with clip timing."""
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid start_date '{start_date}'. Expected YYYY-MM-DD.",
        )

    result = get_cart_event_videos(
        api_key=API_KEY,
        base_id=BASE_ID,
        cage_id=cage_id,
        box_id=box_id,
        start_date=start_date,
        experiment_id_override=(experiment_id or "").strip() or None,
        metadata_bucket="rodent-party",
        metadata_key="internal/metadata/cart_event_metadata.csv",
    )

    if result.get("success"):
        return result

    status_code = int(result.get("status_code") or 500)
    raise HTTPException(
        status_code=status_code,
        detail=result.get("error") or "Unknown error",
    )


@router.get("/cart-clip")
def get_cart_clip(
    cage_id: str = Query(...),
    box_id: str = Query(...),
    start_date: str = Query(...),
    kind: str = Query(...),
    experiment_id: Optional[str] = Query(None),
) -> FileResponse:
    """Stream a trimmed MP4 clip for cart_box or cart_injection."""
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid start_date '{start_date}'. Expected YYYY-MM-DD.",
        )

    from ..services.box_room_service import generate_cart_event_clip_file

    result = generate_cart_event_clip_file(
        api_key=API_KEY,
        base_id=BASE_ID,
        cage_id=cage_id,
        box_id=box_id,
        start_date=start_date,
        kind=kind,
        experiment_id_override=(experiment_id or "").strip() or None,
        metadata_bucket="rodent-party",
        metadata_key="internal/metadata/cart_event_metadata.csv",
    )

    if not result.get("success"):
        raise HTTPException(
            status_code=int(result.get("status_code") or 500),
            detail=result.get("error") or "Unknown error",
        )

    file_path = result.get("file_path")
    if not file_path:
        raise HTTPException(
            status_code=500,
            detail="Clip generation succeeded but file_path is missing",
        )

    return FileResponse(file_path, media_type="video/mp4")
