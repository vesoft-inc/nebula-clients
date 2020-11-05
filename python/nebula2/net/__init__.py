#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


import threading
import logging
import time
import socket

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
    NotValidConnectionException,
    InValidHostname
)

from nebula2.data.ResultSet import ResultSet


class Session(object):
    def __init__(self, connection, session_id, pool, retry_connect=True):
        self.session_id = session_id
        self._connection = connection
        self._timezone = 0
        self._pool = pool
        self._retry_connect = retry_connect

    def execute(self, stmt):
        """
        execute statement
        :param stmt: the ngql
        :return: ResultSet
        """
        try:
            return ResultSet(self._connection.execute(self.session_id, stmt))
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
                if self._retry_connect:
                    if not self._reconnect():
                        logging.warning('Retry connect failed')
                        raise IOErrorException(IOErrorException.E_ALL_BROKEN, 'All connections are broken')
                    try:
                        return ResultSet(self._connection.execute(self.session_id, stmt))
                    except Exception:
                        raise
            raise
        except Exception:
            raise

    def release(self):
        """
        release the connection to pool
        """
        self._connection.is_used = False
        self._connection.signout(self.session_id)
        self._connection.reset()

    def ping(self):
        """
        check the connection is ok
        """
        self._connection.ping()

    def _reconnect(self):
        try:
            conn = self._pool.get_connection()
            if conn is None:
                return False
            self._connection = conn
        except NotValidConnectionException:
            return False
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class ConnectionPool(object):
    S_OK = 0
    S_BAD = 1

    def __init__(self):
        # all addresses of servers
        self._addresses = list()

        # server's status
        self._addresses_status = dict()

        # all connections
        self._connections = dict()
        self._configs = None
        self._lock = RLock()
        self._pos = -1
        self._check_delay = 60  # unit seconds

    def init(self, addresses, configs):
        """
        init the connection pool
        :param addresses: the graphd servers' addresses
        :param configs: the config
        :return: if all addresses are ok, return True else return False.
        """
        self._configs = configs
        for address in addresses:
            if address not in self._addresses:
                try:
                    ip = socket.gethostbyname(address[0])
                except Exception:
                    raise InValidHostname(str(address[0]))
                self._addresses.append((ip, address[1]))
            self._addresses_status[address] = self.S_BAD
            self._connections[address] = deque()

        # detect the services
        self._period_detect()

        # init min connections
        ok_num = self.get_ok_servers_num()
        if ok_num < len(self._addresses):
            return False

        conns_per_address = int(self._configs.min_connection_pool_size / ok_num)
        for addr in self._addresses:
            for i in range(0, conns_per_address):
                connection = Connection()
                connection.open(addr[0], addr[1], self._configs.timeout)
                self._connections[addr].append(connection)
        return True

    def get_session(self, user_name, password, retry_connect=True):
        """
        get session
        :param user_name:
        :param password:
        :param retry_connect: if auto retry connect
        :return: void
        """
        connection = self.get_connection()
        if connection is None:
            raise NotValidConnectionException()
        try:
            session_id = connection.authenticate(user_name, password)
            return Session(connection, session_id, self, retry_connect)
        except Exception:
            raise

    def get_connection(self):
        """
        get available connection
        :return: Connection Object
        """
        with self._lock:
            try:
                ok_num = self.get_ok_servers_num()
                if ok_num == 0:
                    return None
                max_con_per_address = int(self._configs.max_connection_pool_size / ok_num)
                try_count = 0
                while try_count <= len(self._addresses):
                    self._pos = (self._pos + 1) % len(self._addresses)
                    addr = self._addresses[self._pos]
                    if self._addresses_status[addr] == self.S_OK:
                        for connection in self._connections[addr]:
                            if not connection.is_used:
                                connection.is_used = True
                                logging.info('Get connection to {}'.format(addr))
                                return connection

                        if len(self._connections[addr]) < max_con_per_address:
                            connection = Connection()
                            connection.open(addr[0], addr[1], self._configs.timeout)
                            connection.is_used = True
                            self._connections[addr].append(connection)
                            logging.info('Get connection to {}'.format(addr))
                            return connection
                    else:
                        for connection in self._connections[addr]:
                            if not connection.is_used:
                                self._connections[addr].remove(connection)
                    try_count = try_count + 1
                return None
            except Exception:
                raise

    def ping(self, address):
        """
        check the server is ok
        :param address: the server address want to connect
        :return: True or False
        """
        try:
            conn = Connection()
            conn.open(address[0], address[1], 1000)
            conn.close()
            return True
        except Exception as ex:
            logging.error('Connect {}:{} failed: {}'.format(address[0], address[1], ex))
            return False

    def close(self):
        """
        close all connections in pool
        :return: void
        """
        with self._lock:
            for addr in self._connections.keys():
                for connection in self._connections[addr]:
                    connection.close()

    def connnects(self):
        """
        get the number of existing connections
        :return: int
        """
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                count = count + len(self._connections[addr])
            return count

    def in_used_connects(self):
        """
        get the number of the used connections
        :return: int
        """
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                for connection in self._connections[addr]:
                    if connection.is_used:
                        count = count + 1
            return count

    def get_ok_servers_num(self):
        """
        get the number of the ok servers
        :return: int
        """
        with self._lock:
            count = 0
            for addr in self._addresses_status.keys():
                if self._addresses_status[addr] == self.S_OK:
                    count = count + 1
            return count

    def update_servers_status(self):
        """
        update the servers' status
        """
        with self._lock:
            for address in self._addresses:
                if self.ping(address):
                    self._addresses_status[address] = self.S_OK
                else:
                    self._addresses_status[address] = self.S_BAD

    def _remove_idle_connection(self):
        with self._lock:
            for addr in self._connections.keys():
                conns = self._connections[addr]
                for connection in conns:
                    if not connection.is_used and connection.idle_time() > self._configs.idle_time:
                        conns.remove(connection)

    def _period_detect(self):
        self.update_servers_status()
        self._remove_idle_connection()
        timer = threading.Timer(self._check_delay, self._period_detect)
        timer.setDaemon(True)
        timer.start()


class Connection(object):
    is_used = False

    def __init__(self):
        self._connection = None
        self.start_use_time = 0

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

    def authenticate(self, user_name, password):
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

    def signout(self, session_id):
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

    def ping(self):
        try:
            self._connection.ping()
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise

    def reset(self):
        self.start_use_time = time.time()

    def idle_time(self):
        if not self.is_used:
            return 0
        return time.time() - self.start_use_time

