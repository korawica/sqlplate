# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import os
from typing import Any


def config():
    """Return a new Config object"""

    class Config:
        etl_columns: list[str] = [
            os.getenv("ETL_LOAD_SRC_COL", "load_src"),
            os.getenv("ETL_LOAD_ID_COL", "load_id"),
            os.getenv("ETL_LOAD_DATE_COL", "load_date"),
            os.getenv("ETL_UPDT_LOAD_SRC_COL", "updt_load_src"),
            os.getenv("ETL_UPDT_LOAD_ID_COL", "updt_load_id"),
            os.getenv("ETL_UPDT_LOAD_DATE_COL", "updt_load_date"),
        ]

        scd1_soft_delete_columns: list[str] = [
            os.getenv("SCD1_SOFT_DELETE_COL", "delete_f")
        ] + etl_columns

        scd2_columns: list[str] = [
            os.getenv("SCD2_START_DT_COL", "start_date"),
            os.getenv("SCD2_END_DT_COL", "end_date"),
            os.getenv("SCD2_DELETE_COL", "delete_f"),
        ] + etl_columns

        @classmethod
        def remove_sys_cols(cls, columns: list[str]):
            return [col for col in columns if col not in cls.scd2_columns]

        @classmethod
        def export(cls, template_type: str | None = None) -> dict[str, Any]:
            template_type = template_type or 'NOT_SET'
            etl_vars: dict[str, Any] = {}
            if template_type == 'etl':
                etl_vars: dict[str, Any] = {
                    "etl_columns": cls.etl_columns,
                    "scd1_soft_delete_columns": cls.scd1_soft_delete_columns,
                    "scd2_columns": cls.scd2_columns,
                    "only_main": False,
                }

            return {"only_main": False} | etl_vars

    return Config