/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class MockNebulaGraph {

    public static void createGraph() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(100);
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 3699));
        NebulaPool pool = new NebulaPool();
        Session session = null;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("root", "nebula", false);
            String createSchema = "CREATE SPACE IF NOT EXISTS test1(partition_num=10); "
                    + "USE test1;"
                    + "CREATE TAG IF NOT EXISTS person(name string, age int);"
                    + "CREATE EDGE IF NOT EXISTS friend(duration int,degree int,relation string)";
            ResultSet resp = session.execute(createSchema);
            if (!resp.isSucceeded()) {
                pool.close();
                System.exit(1);
            }
        } catch (UnknownHostException | NotValidConnectionException
                | IOErrorException | AuthFailedException e) {
            e.printStackTrace();
        }

        String insertVertexes = "INSERT VERTEX person(name, age) VALUES "
                + "'1':('Bob', 10), "
                + "'2':('Lily', 9), "
                + "'3':('Tom', 10), "
                + "'4':('Jerry', 13), "
                + "'5':('John', 11), "
                + "'6':('Jena', 15), "
                + "'7':('Tina', 18);";
        try {
            ResultSet resp = session.execute(insertVertexes);
        } catch (IOErrorException e) {
            e.printStackTrace();
        }

        String insertEdges = "insert edge friend(duration,degree,relation) values "
                + "\"1\"->\"2\":(10,22,\"girl friend\")"
                + "\"3\"->\"4\":(12,50,\"couple friend\")"
                + "\"5\"->\"6\":(13,99,\"best friend\")"
                + "\"1\"->\"7\":(22,15,\"friend3\")";
        try {
            ResultSet resp = session.execute(insertEdges);
        } catch (IOErrorException e) {
            e.printStackTrace();
        }
        pool.close();
    }
}
