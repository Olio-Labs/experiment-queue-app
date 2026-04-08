import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, Set

from pyairtable import Api

logger = logging.getLogger(__name__)


class PlannerRepository:
    def __init__(self, api_key: str, base_id: str, table_name: str):
        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        self.table = Api(api_key).table(base_id, table_name)

    def get_existing_syringe_colors_by_date(self) -> Dict[date, Set[str]]:
        existing_daily_colors: Dict[date, Set[str]] = defaultdict(set)
        try:
            today_str = datetime.now().date().strftime('%Y-%m-%d')
            filter_formula = f"{{start_date}} >= '{today_str}'"
            fields_to_retrieve = ['start_date', 'syringe_color']
            records = self.table.all(formula=filter_formula, fields=fields_to_retrieve)
            for record in records:
                fields = record.get('fields', {})
                start_date_raw = fields.get('start_date')
                syringe_color = fields.get('syringe_color')
                if not start_date_raw or not syringe_color:
                    continue
                # Normalize possibly-list start_date
                if isinstance(start_date_raw, list):
                    if not start_date_raw:
                        continue
                    start_date_str = start_date_raw[0]
                else:
                    start_date_str = start_date_raw
                try:
                    parsed_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                    # Handle syringe_color as multiselect (list) or single value
                    if isinstance(syringe_color, list):
                        for color in syringe_color:
                            if color:  # Only add non-empty colors
                                existing_daily_colors[parsed_date].add(color)
                    else:
                        existing_daily_colors[parsed_date].add(syringe_color)
                except Exception as e:
                    logger.warning(f"Could not parse start_date '{start_date_raw}' for record {record.get('id')}: {e}")
                    continue
        except Exception as e:
            logger.error(f"PlannerRepository error fetching syringe colors: {e}")
        return existing_daily_colors


