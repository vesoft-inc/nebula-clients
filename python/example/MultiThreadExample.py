#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import time
import threading


sys.path.insert(0, '../')


from nebula2.net import ConnectionPool
from nebula2.Config import Config
from FormatResp import print_resp


def main_test():
    client = None
    try:
        space_name = 'space_' + threading.current_thread().getName()
        print('thread name: %s, space_name : %s' %
                (threading.current_thread().getName(), space_name))
        # Get one client
        client = connection_pool.get_session('root', 'nebula')
        assert client is not None

        # Create space mySpace and schema
        client.execute('CREATE SPACE IF NOT EXISTS {}; USE {};'
                       'CREATE TAG IF NOT EXISTS person(name string, age int);'
                       'CREATE EDGE IF NOT EXISTS like(likeness double);'
                       .format(space_name, space_name))
        time.sleep(6)

        # Insert vertexes
        client.execute('INSERT VERTEX person(name, age) VALUES '
                       '\'Bob\':(\'Bob\', 10), '
                       '\'Lily\':(\'Lily\', 9), '
                       '\'Tom\':(\'Tom\', 10), '
                       '\'Jerry\':(\'Jerry\', 13), '
                       '\'John\':(\'John\', 11)')

        # Insert edges
        client.execute('INSERT EDGE like(likeness) VALUES '
                       '\'Bob\'->\'Lily\':(80.0), '
                       '\'Bob\'->\'Tom\':(70.0), '
                       '\'Lily\'->\'Jerry\':(84.0), '
                       '\'Tom\'->\'Jerry\':(68.3), '
                       '\'Bob\'->\'John\':(97.2)')

        # Query data
        query_resp = client.execute('GO FROM \"Bob\" OVER like YIELD $^.person.name, '
                                    '$^.person.age, like.likeness')
        if query_resp.error_code:
            print('Execute failed: %s' % query_resp.error_msg)
            exit(1)

        # Print the result of query
        print(' \n====== The query result of thread[%s]======\n '
              % threading.current_thread().getName())
        print_resp(query_resp)
        client.release()

    except Exception as x:
        print(x)
        if client is not None:
            client.release()
        exit(1)


if __name__ == '__main__':
    config = Config()
    config.timeout = 1000
    config.max_connection_pool_size = 4
    config.max_retry_time = 3

    addresses = list()
    addresses.append(('127.0.0.1', 3699))
    # init connection pool
    connection_pool = ConnectionPool()
    assert connection_pool.init(addresses, config)

    # Use multi thread and reuse the session three times
    for count in range(0, 3):
        threads = list()
        for i in range(0, 4):
            threads.append(threading.Thread(target=main_test, name='thread{}'.format(i)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    # close connect pool
    connection_pool.close()
