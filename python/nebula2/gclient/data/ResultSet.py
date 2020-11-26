#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula2.graph import ttypes
from nebula2.Exception import (
    OutOfRangeException,
    InvalidKeyException
)

from nebula2.gclient.data.DataObject import ValueWrapper


class Record(object):
    def __init__(self, values, names):
        assert len(names) == len(values)
        self._record = list()
        self._names = names

        for val in values:
            self._record.append(ValueWrapper(val))

    def __iter__(self):
        return iter(self._record)

    def size(self):
        return len(self._names)

    def get_value(self, index):
        '''
        get value by index
        :return: Value
        '''
        if index >= len(self._names):
            raise OutOfRangeException()
        return self._record[index]

    def get_value(self, key):
        '''
        get value by key
        :return: Value
        '''
        if key not in self._names:
            raise InvalidKeyException(key)
        return self._record[self._names[key]]

    def values(self):
        return self._record

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._record)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ResultSet(object):
    def __init__(self, resp, decode_type='utf-8'):
        '''
        get data from ResultSet
        '''
        self._decode_type = decode_type
        self._resp = resp
        self._pos = -1
        self._key_indexes = {}
        self._column_names = []
        if self._resp.data is not None:
            for index, name in enumerate(self._resp.data.column_names):
                d_name = name.decode(self._decode_type)
                self._column_names.append(d_name)
                self._key_indexes[d_name] = index

    def is_succeeded(self):
        return self._resp.error_code == ttypes.ErrorCode.SUCCEEDED

    def error_code(self):
        return self._resp.error_code

    def space_name(self):
        if self._resp.space_name is None:
            return ''
        return self._resp.space_name.decode(self._decode_type)

    def error_msg(self):
        if self._resp.error_msg is None:
            return ''
        return self._resp.error_msg.decode(self._decode_type)

    def comment(self):
        return self._resp.comment.decode(self._decode_type)

    def latency(self):
        '''
        unit us
        '''
        return self._resp.latency_in_us

    def plan_desc(self):
        return self._resp.plan_desc

    def is_empty(self):
        return self._resp.data is None or len(self._resp.data.rows) == 0

    def keys(self):
        '''
        get colNames
        '''
        if self._resp.data is None:
            raise RuntimeError('Data is none')
        return self._column_names

    def row_size(self):
        '''
        get one row size
        '''
        if self._resp.data is None:
            raise RuntimeError('Data is none')
        return len(self._resp.data.rows)

    def col_size(self):
        '''
        get one col size
        '''
        if self._resp.data is None:
            raise RuntimeError('Data is none')
        return len(self._column_names)

    def get_row_types(self):
        '''
        Get row types
        :param empty
        :return: list<int>
          ttypes.Value.__EMPTY__ = 0
          ttypes.Value.NVAL = 1
          ttypes.Value.BVAL = 2
          ttypes.Value.IVAL = 3
          ttypes.Value.FVAL = 4
          ttypes.Value.SVAL = 5
          ttypes.Value.DVAL = 6
          ttypes.Value.TVAL = 7
          ttypes.Value.DTVAL = 8
          ttypes.Value.VVAL = 9
          ttypes.Value.EVAL = 10
          ttypes.Value.PVAL = 11
          ttypes.Value.LVAL = 12
          ttypes.Value.MVAL = 13
          ttypes.Value.UVAL = 14
          ttypes.Value.GVAL = 15
        '''
        if self._resp.data is None:
            return []
        return [(value.getType()) for value in self._resp.data.rows[0].values]

    def row_values(self, row_index):
        '''
        Get row values
        :param index: the Record index
        :return: list<ValueWrapper>
        '''
        if self._resp.data is None:
            raise RuntimeError('Data is none')
        if row_index >= len(self._resp.data.rows):
            raise OutOfRangeException()
        return [(ValueWrapper(value)) for value in self._resp.data.rows[row_index].values]

    def column_values(self, key):
        '''
        get column values
        :param key: the col name
        :return: list<ValueWrapper>
        '''
        if self._resp.data is None:
            raise RuntimeError('Data is none')
        if key not in self._column_names:
            raise InvalidKeyException(key)

        return [(ValueWrapper(row.values[self._key_indexes[key]])) for row in self._resp.data.rows]

    def rows(self):
        '''
        get all rows
        :param key: empty
        :return: list<Row>
        '''
        if self._resp.data is None:
            return None
        return self._resp.data.rows

    def __iter__(self):
        return self

    def __next__(self):
        '''
        The record iterator
        :return: recode
        '''
        if len(self._resp.data.rows) == 0 or self._pos >= len(self._resp.data.rows) - 1:
            raise StopIteration
        self._pos = self._pos + 1
        return Record(self._resp.data.rows[self._pos].values, self._column_names)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._resp)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


