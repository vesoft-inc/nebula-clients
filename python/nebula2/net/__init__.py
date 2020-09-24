#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from enum import Enum
from collections import deque
from threading import RLock

from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TTransportException
from thrift.protocol import TBinaryProtocol

from nebula2.graph import (
    ttypes,
    GraphService
)

from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
    NotValidConnectionException
)

from nebula2.data.ResultSet import ResultSet


class Session(object):
    def __init__(self, connection, session_id, pool):
        self._session_id = session_id
        self._connection = connection
        self._timezone = 0
        self._pool = pool

    def execute(self, stmt):
        try:
            return ResultSet(self._connection.execute(self._session_id, stmt))
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
            raise
        except Exception:
            raise

    def release(self):
        self._connection.is_used = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class ConnectionPool(object):
    S_OK = 0
    S_BAD = 1

    def __init__(self, addresses, user_name, password, configs):
        # all addresses of servers
        self._addresses = list()
        for addr in addresses:
            if addr not in self._addresses:
                self._addresses.append(addr)

        # all session_id
        self._session_ids = list()

        # server's status
        self._addresses_status = dict()
        # all connections
        self._connections = dict()
        for address in addresses:
            self._addresses_status[address] = self.S_BAD
            self._connections[address] = deque()
        self._configs = configs
        self._user_name = user_name
        self._password = password
        self._lock = RLock()
        self._pos = -1

        self.update_servers_status()

    def get_session(self):
        conn, session_id = self.chosen_connection()
        if conn is None:
            raise NotValidConnectionException()
        return Session(conn, session_id, self)

    def chosen_connection(self):
        with self._lock:
            try:
                max_con_per_address = int(self._configs.max_connection_pool_size / self.get_ok_servers_num())
                try_count = 0
                while try_count <= len(self._addresses):
                    self._pos = (self._pos + 1) % len(self._addresses)
                    addr = self._addresses[self._pos]
                    if self._addresses_status[addr] == self.S_OK:
                        if len(self._connections[addr]) < max_con_per_address:
                            use_conn = UseConnection()
                            use_conn.connection = Connection()
                            use_conn.connection.open(addr[0], addr[1], self._configs.timeout)
                            use_conn.session_id = use_conn.connection.auth(self._user_name, self._password)
                            self._connections[addr].append(use_conn)
                        for conn in self._connections[addr]:
                            if not conn.connection.is_used:
                                conn.connection.is_used = True
                                return conn.connection, conn.session_id
                    else:
                        for conn in self._connections[addr]:
                            if not conn.connection.is_used:
                                self._connections[addr].remove(conn)
                    try_count = try_count + 1
                return None, 0
            except Exception:
                raise

    def ping(self, address):
        try:
            conn = Connection()
            conn.open(address[0], address[1], 1000)
            return True
        except Exception:
            return False

    def close(self):
        with self._lock:
            for addr in self._connections.keys():
                for conn in self._connections[addr]:
                    conn.connection.release(conn.session_id)
                    conn.connection.close()

    def connnects(self):
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                for conn in self._connections[addr]:
                    if conn.connection.is_used:
                        count = count + 1
            return count

    def in_used_connects(self):
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                for conn in self._connections[addr]:
                    if conn.connection.is_used:
                        count = count + 1
            return count

    def get_ok_servers_num(self):
        with self._lock:
            count = 0
            for addr in self._addresses_status.keys():
                if self._addresses_status[addr] == self.S_OK:
                    count = count + 1
            return count

    def update_servers_status(self):
        with self._lock:
            for address in self._addresses:
                if self.ping(address):
                    self._addresses_status[address] = self.S_OK


class UseConnection(object):
    session_id = 0
    connection = None


class Connection(object):
    is_used = False

    def __init__(self):
        self._connection = None

    def open(self, ip, port, timeout):
        try:
            s = TSocket.TSocket(ip, port)
            if timeout > 0:
                s.setTimeout(timeout)
            transport = TTransport.TBufferedTransport(s)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self._connection = GraphService.Client(protocol)
        except Exception:
            raise

    def auth(self, user_name, password):
        try:
            resp = self._connection.authenticate(user_name, password)
            if resp.error_code != ttypes.ErrorCode.SUCCEEDED:
                raise AuthFailedException(resp.error_msg)
            return resp.session_id
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise IOErrorException(IOErrorException.E_CONNECT_BROKEN)

    def execute(self, session_id, stmt):
        try:
            resp = self._connection.execute(session_id, stmt)
            return resp
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise IOErrorException(IOErrorException.E_CONNECT_BROKEN)

    def release(self, session_id):
        try:
            self._connection.signout(session_id)
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise

    def close(self):
        try:
            self._connection._iprot.trans.close()
        except Exception:
            raise

