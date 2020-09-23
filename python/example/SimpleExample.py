#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import time

sys.path.insert(0, '../')

from nebula2.net import ConnectionPool
from nebula2.Config import Config
from FormatResp import FormatResp

if __name__ == '__main__':
    config = Config()
    config.timeout = 1000
    config.max_connection_pool_size = 4
    config.max_retry_time = 3

    addresses = list()
    addresses.append(('192.168.8.6', 3777))
    # init connection pool
    connection_pool = ConnectionPool(addresses, 'root', 'nebula', config)

    # get session from the pool
    client = connection_pool.get_session()
    assert client is not None

    client.execute('CREATE SPACE IF NOT EXISTS test; USE test;'
                   'CREATE TAG IF NOT EXISTS person(name string, age int);')
    
    # insert data need to sleep after create schema
    time.sleep(6)

    # insert vertex
    client.execute('INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10)')

    # get vertex
    resp = client.execute('FETCH PROP ON person "Bob"')
    FormatResp.print_resp(resp)

