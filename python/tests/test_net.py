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
    ConnectionPool,
    AsyncConnection
)

from nebula2.graph import ttypes
from nebula2.Config import Config

from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
    NotValidConnectionException,
    InValidHostname
)


class TestConnection(TestCase):
    def test_create(self):
        try:
            conn = Connection()
            conn.open('127.0.0.1', 3699, 1000)
            session_id = conn.authenticate('root', 'nebula')
            assert session_id != 0
            conn.close()
        except Exception as ex:
            assert False, ex

    def test_release(self):
        try:
            conn = Connection()
            conn.open('127.0.0.1', 3699, 1000)
            session_id = conn.authenticate('root', 'nebula')
            assert session_id != 0
            resp = conn.execute(session_id, 'SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg
            conn.signout(session_id)
            resp = conn.execute(session_id, 'SHOW SPACES')
            assert resp.error_code != ttypes.ErrorCode.SUCCEEDED
        except Exception as ex:
            assert False, ex

    def test_close(self):
        conn = Connection()
        conn.open('127.0.0.1', 3699, 1000)
        session_id = conn.authenticate('root', 'nebula')
        assert session_id != 0
        conn.close()
        try:
            conn.authenticate('root', 'nebula')
        except Exception as ex:
            assert True


class TestConnectionPool(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(('127.0.0.1', 3699))
        self.addresses.append(('127.0.0.1', 3700))
        self.configs = Config()
        self.configs.min_connection_pool_size = 2
        self.configs.max_connection_pool_size = 4
        self.configs.max_retry_time = 3
        self.configs.timeout = 10000
        self.configs.idle_time = 2*1000
        self.pool = ConnectionPool()
        assert self.pool.init(self.addresses, self.configs)
        assert self.pool.connnects() == 2

    def test_ping(self):
        assert self.pool.ping(('127.0.0.1', 3699))
        assert self.pool.ping(('127.0.0.1', 5000)) is False

    def test_init_failed(self):
        # init succeeded
        pool1 = ConnectionPool()
        addresses = list()
        addresses.append(('127.0.0.1', 3699))
        addresses.append(('127.0.0.1', 3700))
        assert pool1.init(addresses, Config())

        # init failed, connected failed
        pool2 = ConnectionPool()
        addresses = list()
        addresses.append(('127.0.0.1', 3800))
        assert not pool2.init(addresses, Config())

        # init failed, hostname not existed
        try:
            pool3 = ConnectionPool()
            addresses = list()
            addresses.append(('not_exist_hostname', 3800))
            assert not pool3.init(addresses, Config())
        except InValidHostname:
            assert True, "We expected get the exception"

    def test_get_session(self):
        # get session succeeded
        sessions = list()
        for num in range(0, self.configs.max_connection_pool_size):
            session = self.pool.get_session('root', 'nebula')
            resp = session.execute('SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
            sessions.append(session)

        # get session failed
        try:
            self.pool.get_session('root', 'nebula')
        except NotValidConnectionException:
            assert True

        assert self.pool.in_used_connects() == 4
        # release session
        for session in sessions:
            session.release()

        assert self.pool.in_used_connects() == 0

        # test get session after release
        for num in range(0, self.configs.max_connection_pool_size - 1):
            session = self.pool.get_session('root', 'nebula')
            resp = session.execute('SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
            sessions.append(session)

        assert self.pool.in_used_connects() == 3

    def test_stop_close(self):
        session = self.pool.get_session('root', 'nebula')
        assert session is not None
        resp = session.execute('SHOW SPACES')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
        self.pool.close()
        try:
            session.execute('SHOW SPACES')
        except Exception:
            assert True


class TestSession(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(('127.0.0.1', 3699))
        self.addresses.append(('127.0.0.1', 3700))
        self.user_name = 'root'
        self.password = 'nebula'
        self.configs = Config()
        self.configs.min_connection_pool_size = 2
        self.configs.max_connection_pool_size = 4
        self.configs.max_retry_time = 3
        self.configs.timeout = 10000
        self.configs.idle_time = 2*1000
        self.pool = ConnectionPool()
        self.pool._check_delay = 2
        assert self.pool.init(self.addresses, self.configs)
        assert self.pool.connnects() == 2

    def test_reconnect(self):
        try:
            import time
            session = self.pool.get_session('root', 'nebula')
            for i in range(0, 1):
                session.execute('SHOW SPACES')
                time.sleep(2)
            new_session = self.pool.get_session('root', 'nebula')
            new_session.execute('SHOW SPACES')
        except Exception:
            assert False


class TestAsyncConnection(TestCase):
    def test_create(self):
        import asyncio
        conn = AsyncConnection()
        try:
            conn.open('127.0.0.1', 3699, 1000)
            session_id = conn.authenticate('root', 'nebula')
            assert session_id != 0

            self.async_result = None

            def callback(resp):
                self.async_result = resp

            resp = conn.execute(session_id, "SHOW HOSTS;")
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
            assert resp.data is not None
            assert len(resp.data.rows) == 3
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(asyncio.wait([conn.async_execute(session_id, "SHOW HOSTS;", callback)]))
            assert self.async_result.error_code == ttypes.ErrorCode.SUCCEEDED
            assert len(self.async_result.data.rows) == 3

            conn.async_execute(session_id, "SHOW HOSTS;", callback)
        except Exception as ex:
            assert False, ex
        finally:
            conn.close()

    def test_release(self):
        try:
            conn = AsyncConnection()
            conn.open('127.0.0.1', 3699, 1000)
            session_id = conn.authenticate('root', 'nebula')
            assert session_id != 0
            resp = conn.execute(session_id, 'SHOW SPACES')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg
            conn.signout(session_id)
            resp = conn.execute(session_id, 'SHOW SPACES')
            assert resp.error_code != ttypes.ErrorCode.SUCCEEDED
        except Exception as ex:
            assert False, ex

    def test_ping(self):
        try:
            conn = AsyncConnection()
            conn.open('127.0.0.1', 3699, 1000)
            assert conn.ping()
        except Exception as ex:
            assert False, ex

    def test_close(self):
        conn = AsyncConnection()
        conn.open('127.0.0.1', 3699, 1000)
        session_id = conn.authenticate('root', 'nebula')
        assert session_id != 0
        conn.close()
        try:
            conn.authenticate('root', 'nebula')
        except Exception as ex:
            assert True
