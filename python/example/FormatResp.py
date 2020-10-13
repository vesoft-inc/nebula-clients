#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import prettytable

from nebula2.common import ttypes


def print_resp(resp):
    output_table = prettytable.PrettyTable()
    output_table.field_names = resp.keys()
    for recode in resp:
        value_list = []
        for col in recode:
            if col.getType() == ttypes.Value.__EMPTY__:
                value_list.append('__EMPTY__')
            elif col.getType() == ttypes.Value.NVAL:
                value_list.append('__NULL__')
            elif col.getType() == ttypes.Value.BVAL:
                value_list.append(col.get_bVal())
            elif col.getType() == ttypes.Value.IVAL:
                value_list.append(col.get_iVal())
            elif col.getType() == ttypes.Value.FVAL:
                value_list.append(col.get_fVal())
            elif col.getType() == ttypes.Value.SVAL:
                value_list.append(col.get_sVal())
            elif col.getType() == ttypes.Type.TVAL:
                value_list.append(col.get_tVal())
            elif col.getType() == ttypes.Type.DVAL:
                value_list.append(col.get_dVal())
            elif col.getType() == ttypes.Type.DATETIME:
                value_list.append(col.get_tVal())
            elif col.getType() == ttypes.Type.LVAL:
                value_list.append(col.get_lVal())
            elif col.getType() == ttypes.Type.UVAL:
                value_list.append(col.get_SVal())
            elif col.getType() == ttypes.Type.MVAL:
                value_list.append(col.get_tVal())
            elif col.getType() == ttypes.Type.VVAL:
                value_list.append(col.get_vVal())
            elif col.getType() == ttypes.Type.EVAL:
                value_list.append(col.get_eVal())
            elif col.getType() == ttypes.Type.PVAL:
                value_list.append(col.get_pVal())
            else:
                print('ERROR: Type unsupported')
                return
        output_table.add_row(value_list)
    print(output_table)
