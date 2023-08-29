import math
import requests
from typing import Any, List, Dict, Union

import numpy as np
import pandas as pd



URL = "https://data.sfgov.org/resource/g8m3-pdis.json"


class SanFranciscoBusinessData:
    def __init__(self, url: str = URL):
        self.url = url

    def get_data(self) -> List[dict]:
        raw_data: List[dict] = self._get_data_from_url()
        records = self._to_records(raw_data)

        return records

    def _get_data_from_url(self):
        """Pull data from API endpoint"""
        response = requests.get(self.url)
        return response.json()


    def _to_record(self, item: dict) -> dict:
        record = {
            self._transform_key(k):v for k, v in item.items() if k != "location"
        }

        lat, lon = item["location"]["coordinates"]
        location = {
            "latitude": lat,
            "longitude": lon
        }

        record = {**record, **location}

        return record

    def _transform_key(self, key: str) -> str:
        if key.startswith(":@"):
            return key[2:]

        return key

    def _to_records(self, items: List[dict]) -> List[dict]:
        return [self._to_record(item) for item in items]



class RecordsToInsertSQL:
    def to_sql(self, records: List[dict], table_name: str) -> str:
        split_records = self._normalize_records(records)
        columns, data = split_records["columns"], split_records["data"]
        sql: str = f"""
            INSERT INTO {table_name} ({self._get_columns_sql(columns)}) 
            VALUES {self._get_values_sql(columns, data)}
        """

        return sql
    
    def _get_columns_sql(self, columns: List[str]) -> str:
        return ", ".join(columns)
    
    def _get_values_sql(self, columns: List[str], rows: List[Any]) -> str:
        rows_sql: List[str] = [self._get_row_sql(columns, row_values) for row_values in rows]
        return ", ".join(rows_sql)
    
    def _get_row_sql(self, columns: List[str], row_values: List[Any]) -> str:
        return "(" + ", ".join([self._value_to_sql(columns, index, value) for index, value in enumerate(row_values)]) + ")"

    def _value_to_sql(self, keys: List[str], index: int, value):
        key = keys[index]
        if self._is_int_column(key):
            return self._number_or_null(value)
        elif self._is_bool_column(key):
            return str(value)
        elif self._is_lat_lon(key):
            return str(value)
        elif type(value) == float:
            return self._number_or_null(value)

        return f"'{str(value)}'"

    def _normalize_records(self, records: List[dict]) -> List[dict]:
        normalized = pd.DataFrame(records).to_dict(orient="split")

        return normalized

    def _is_int_column(self, key: str):
        int_columns = [
	        "supervisor_district",
	        "computed_region_6qbp_sg9q",
	        "computed_region_qgnn_b9vv",
	        "computed_region_26cr_cadq",
	        "computed_region_ajp5_b2md",
	        "computed_region_jwn9_ihcz",
        ]

        return key in int_columns

    def _is_lat_lon(self, key: str) -> bool:
        lat_lon_columns = [
            "latitude",
            "longitude",
        ]

        return key in lat_lon_columns
    
    def _is_bool_column(self, key: str):
        bool_columns = [
            "parking_tax",
	        "transient_occupancy_tax",
        ]

        return key in bool_columns

    def _number_or_null(self, value: Union[int, float]):
        try:
            if math.isnan(value):
                return "NULL"
        except:
            return str(value)
        
        return str(value)
