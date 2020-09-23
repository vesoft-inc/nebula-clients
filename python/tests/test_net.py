#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from unittest import TestCase

from nebula2.net import (
    Connection,
    ConnectionPool
)

from nebula2.graph import ttypes
from nebula2.Config import Config

from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
    NotValidConnectionException
)


class TestConnection(TestCase):
    def test_create(self):
        try:
            conn = Connection()
            conn.open('127.0.0.1', 32976, 1000)
            session_id = conn.auth('root', 'nebula')
            assert session_id != 0
            conn.close()
        except Exception as ex:
            assert False, ex

    def test_release(self):
        try:
            conn = Connection()
            conn.open('127.0.0.1', 32976, 1000)
            session_id = conn.auth('root', 'nebula')
            assert session_id != 0
            resp = conn.execute(session_id, 'SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
            conn.release(session_id)
            resp = conn.execute(session_id, 'SHOW SPACES')
            assert resp.error_code != ttypes.ErrorCode.SUCCEEDED
        except Exception as ex:
            assert False, ex

    def test_close(self):
        conn = Connection()
        conn.open('127.0.0.1', 32976, 1000)
        session_id = conn.auth('root', 'nebula')
        assert session_id != 0
        conn.close()
        try:
            conn.auth('root', 'nebula')
        except Exception as ex:
            assert True


class TestConnectionPool(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(('127.0.0.1', 32976))
        self.addresses.append(('192.168.8.6', 3777))
        self.user_name = 'root'
        self.password = 'nebula'
        self.configs = Config()
        self.configs.max_connection_pool_size = 4
        self.configs.max_retry_time = 3
        self.configs.timeout = 10000
        self.pool = ConnectionPool(self.addresses, self.user_name, self.password, self.configs)

    def test_ping(self):
        assert self.pool.ping(('127.0.0.1', 32976))
        assert self.pool.ping(('127.0.0.1', 5000)) is False

    def test_get_session(self):
        # get session succeeded
        sessions = list()
        for num in range(0, self.configs.max_connection_pool_size):
            session = self.pool.get_session()
            resp = session.execute('SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
            sessions.append(session)

        # get session failed
        try:
            self.pool.get_session()
        except NotValidConnectionException:
            assert True

        # release session
        for session in sessions:
            session.release()

        # test get session after release
        for num in range(0, self.configs.max_connection_pool_size - 1):
            session = self.pool.get_session()
            resp = session.execute('SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
            sessions.append(session)

    def test_stop_close(self):
        session = self.pool.get_session()
        resp = session.execute('SHOW SPACES')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
        self.pool.close()
        try:
            session.execute('SHOW SPACES')
        except Exception:
            assert True
