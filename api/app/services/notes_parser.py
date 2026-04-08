"""
Single source of truth for parsing and updating notes strings used for
experiment scheduling. This mirrors and will eventually replace the
helpers `_parse_notes_for_scheduling` and `_update_notes_with_map_for_scheduling`
in the legacy `app.py`.
"""

from __future__ import annotations

import ast
import re
from typing import Dict, List, Optional, Tuple

_MANIPS_LIST_PATTERN = re.compile(r"manips\s*=\s*\[(.*?)\]", re.IGNORECASE | re.DOTALL)
_MAP_PATTERN = re.compile(r"map\s*=\s*(\{.*?\})", re.IGNORECASE | re.DOTALL)


def parse_notes(notes_str: Optional[str]) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    Parse a notes string to extract a direct mapping and a list of manipulation
    custom IDs. Returns (direct_mapping, manip_custom_ids).

    - direct_mapping: dict of custom manipulation id -> list of cage ids
    - manip_custom_ids: list of custom manipulation ids for pseudorandom flows
    """
    if not notes_str or not isinstance(notes_str, str):
        return {}, []

    direct_map: Dict[str, List[str]] = {}
    # Extract mapping first
    map_match = _MAP_PATTERN.search(notes_str)
    if map_match:
        map_str = map_match.group(1)
        try:
            parsed = ast.literal_eval(_sanitize_mapping_literal(map_str))
            if isinstance(parsed, dict):
                # normalize values to list[str]
                for k, v in parsed.items():
                    if isinstance(v, (list, tuple)):
                        direct_map[str(k)] = [str(x) for x in v]
                    elif v is None:
                        direct_map[str(k)] = []
                    else:
                        direct_map[str(k)] = [str(v)]
        except Exception:
            import logging
            logging.getLogger('notes_parser').warning(
                f"Failed to parse notes mapping; leaving direct_map empty. Raw: {map_str[:200]}",
                exc_info=True,
            )

    # Extract manips list
    manip_ids: List[str] = []
    manips_match = _MANIPS_LIST_PATTERN.search(notes_str)
    if manips_match:
        items_str = manips_match.group(1)
        try:
            parsed_list = ast.literal_eval(_sanitize_list_literal(f"[{items_str}]"))
            if isinstance(parsed_list, (list, tuple)):
                manip_ids = [str(x) for x in parsed_list if str(x).strip()]
        except Exception:
            import logging
            logging.getLogger('notes_parser').warning(
                f"Failed to parse manips list; leaving empty. Raw: {items_str[:200]}",
                exc_info=True,
            )
            manip_ids = []

    return direct_map, manip_ids


def update_notes_with_mapping(notes_str: Optional[str], new_map: Dict[str, List[str]]) -> str:
    """
    Update or insert the mapping section in a notes string. Returns the
    updated notes.
    """
    if not notes_str:
        notes_str = ""

    map_literal = _mapping_to_literal(new_map)

    if _MAP_PATTERN.search(notes_str):
        # Replace existing map
        updated = _MAP_PATTERN.sub(f"map = {map_literal}", notes_str)
        return updated
    else:
        # Append new map at the end on a new line
        if notes_str and not notes_str.endswith("\n"):
            notes_str += "\n"
        return f"{notes_str}map = {map_literal}"


def _sanitize_mapping_literal(map_str: str) -> str:
    # Add quotes around unquoted IDs like m0000001 and c0000750 to make it
    # a valid Python literal for ast.literal_eval.
    sanitized = _quote_unquoted_ids(map_str)
    return sanitized


def _sanitize_list_literal(list_str: str) -> str:
    # Add quotes around unquoted IDs like m0000001 and c0000750
    sanitized = _quote_unquoted_ids(list_str)
    return sanitized


def _mapping_to_literal(mapping: Dict[str, List[str]]) -> str:
    normalized = {str(k): [str(x) for x in (v or [])] for k, v in mapping.items()}
    return str(normalized)


_UNQUOTED_ID_PATTERN = re.compile(r"(?<!['\"])\b([mc]\d{7})\b(?!['\"])", re.IGNORECASE)


def _quote_unquoted_ids(s: str) -> str:
    # Wrap bare identifiers like m0000001 or c0000750 with quotes, unless already quoted.
    return _UNQUOTED_ID_PATTERN.sub(r"'\1'", s)


