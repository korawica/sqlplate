# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from typing import Any


class Config:
    etl_columns: list[str] = [
        "load_src",
        "load_id",
        "load_date",
        "updt_load_src",
        "updt_load_id",
        "updt_load_date",
    ]

    scd1_soft_delete_columns: list[str] = ["delete_f"] + etl_columns

    scd2_columns: list[str] = [
        "start_date",
        "end_date",
        "delete_f",
    ] + etl_columns

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
