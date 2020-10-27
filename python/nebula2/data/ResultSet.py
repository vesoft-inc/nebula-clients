#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from collections import deque
from nebula2.data.DataObject import ConvertValue

from nebula2.Exception import (
    OutOfRangeException,
    InvalidKeyException
)


class Record(object):
    def __init__(self, values, names):
        self._record = list()
        self._record = values
        self._size = len(self._record)
        self._names = dict()

        for index, name in enumerate(names, start=0):
            self._names[name] = index

        assert len(self._names) == self._size

    def __iter__(self):
        return iter(self._record)

    def get_type(self, index):
        '''
        get type by index
        :return: value type
        '''
        if index >= self._size:
            raise OutOfRangeException()
        return self._record[index].getType()

    def get_type(self, key):
        '''
        get type by key
        :return: value type
        '''
        if key not in self._names:
            raise InvalidKeyException(key)
        return self._record[self._names[key]].getType()

    def get_value(self, index):
        '''
        get value by index
        :return: Value
        '''
        if index >= self._size:
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

    def get_path(self, index):
        '''
        get path by index
        :return: Path
        '''
        if index >= self._size:
            raise OutOfRangeException()
        if self._record[index] != ttypes.Value.PVAL:
            return InvalidValueTypeException(
                'the type of index: {} is {} != {}'.format(index,
                                                           self._record[index].getType(),
                                                           ttypes.Value.PVAL))
        return ConvertValue.convert(self._record[index])

    def get_path(self, key):
        '''
        get path by key
        :return: Path
        '''
        if key not in self._names:
            raise InvalidKeyException(key)
        if self._names[key] != ttypes.Value.PVAL:
            return InvalidValueTypeException(
                'the type of key: {} is {} != {}'.format(key,
                                                         self._names[key].getType(),
                                                         ttypes.Value.PVAL))
        return ConvertValue.convert(self._names[key])

    def get_relationship(self, index):
        '''
        get relationship by key
        :return: Relationship
        '''
        if index >= self._size:
            raise OutOfRangeException()
        if self._record[index] != ttypes.Value.EVAL:
            return InvalidValueTypeException(
                'the type of index: {} is {} != {}'.format(index,
                                                           self._record[index].getType(),
                                                           ttypes.Value.EVAL))
        return ConvertValue.convert(self._record[index])

    def get_relationship(self, key):
        '''
        get relationship by key
        :return: Relationship
        '''
        if key not in self._names:
            raise InvalidKeyException(key)
        if self._names[key] != ttypes.Value.EVAL:
            return InvalidValueTypeException(
                'the type of key: {} is {} != {}'.format(key,
                                                         self._names[key].getType(),
                                                         ttypes.Value.EVAL))
        return ConvertValue.convert(self._names[key])

    def get_node(self, index):
        '''
        get node by key
        :return: Node
        '''
        if index >= self._size:
            raise OutOfRangeException()
        if self._record[index] != ttypes.Value.VVAL:
            return InvalidValueTypeException(
                'the type of index: {} is {} != {}'.format(index,
                                                           self._record[index].getType(),
                                                           ttypes.Value.VVAL))
        return ConvertValue.convert(self._record[index])

    def get_node(self, key):
        '''
        get node by key
        :return: Node
        '''
        if key not in self._names:
            raise InvalidKeyException(key)
        if self._names[key] != ttypes.Value.VVAL:
            return InvalidValueTypeException(
                'the type of key: {} is {} != {}'.format(key,
                                                         self._names[key].getType(),
                                                         ttypes.Value.VVAL))
        return ConvertValue.convert(self._names[key])

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._record)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ResultSet(object):
    def __init__(self, resp):
        '''
        get data from ResultSet
        '''
        self._keys = list()
        self._records = deque()
        self._size = 0
        self.error_code = resp.error_code
        self.latency_in_us = 0
        if resp.latency_in_us is not None:
            self.latency_in_us = resp.latency_in_us
        self.space_name = resp.space_name
        self.error_msg = resp.error_msg
        self.plan_desc = resp.plan_desc
        self.comment = resp.comment
        if resp.data is not None:
            ds = resp.data
            self._keys = ds.column_names
            self._index = dict()
            for index, name in enumerate(self._keys):
                self._index[name] = index

            for row in ds.rows:
                self._records.append(Record(row.values, self._keys))

            self._size = len(self._records)

    def keys(self):
        return self._keys

    def row_size(self):
        return self._size

    def col_size(self):
        return len(self._keys)

    def row_values(self, index):
        '''
        Get row values
        :param index: the Record index
        :return: Record
        '''
        if index >= self._size:
            raise OutOfRangeException()
        return self._records[index]

    def column_values(self, key):
        '''
        get column values
        :param index: the col name
        :return: list
        '''
        if key not in self._index.keys():
            return None

        return [(record[self._index[key]]) for record in self._records]

    def __iter__(self):
        '''
        The row iterator
        :return: row values
        '''
        return iter(self._records)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._records)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


