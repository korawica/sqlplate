# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations


class BaseConf:
    scd2_columns: list[str] = [
        'start_dt',
        'end_dt',
        'delete_f',
        'prcs_nm',
        'prcs_ld_id',
        'asat_dt',
        'updt_prcs_nm',
        'updt_prcs_ld_id',
        'updt_asat_dt',
    ]
