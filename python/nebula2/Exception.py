#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


class OutOfRangeException(Exception):
    def __init__(self):
        Exception.__init__(self)
        self.message = 'list index out of range'


class InvalidKeyException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message=None)
        self.message = 'KeyError: '.format(message)


class AuthFailedException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message=None)
        self.message = 'Auth failed: '.format(message)


class NotValidConnectionException(Exception):
    def __init__(self):
        Exception.__init__(self)
        self.message = 'No extra connection'


class IOErrorException(Exception):
    E_UNKNOWN = 0
    E_ALL_BROKEN = 1
    E_CONNECT_BROKEN = 2

    def __init__(self, type=E_UNKNOWN, message=None):
        Exception.__init__(self, message)
        self.type = type

