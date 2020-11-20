/*
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph;

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

public class MockNebula {
    public static void createGraph() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(100);
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 3699),
                new HostAddress("127.0.0.1", 3700));
        NebulaPool pool = new NebulaPool();
        Session session = null;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("root", "nebula", false);
            String createSchema = "CREATE SPACE IF NOT EXISTS test; "
                    + "USE test;"
                    + "CREATE TAG IF NOT EXISTS person(name string, age int);"
                    + "CREATE EDGE IF NOT EXISTS like(likeness double)";
            ResultSet resp = session.execute(createSchema);
            if (!resp.isSucceeded()) {
                System.exit(1);
            }
        } catch (UnknownHostException | NotValidConnectionException
                | IOErrorException | AuthFailedException e) {
            e.printStackTrace();
        }

        String insertVertexes = "INSERT VERTEX person(name, age) VALUES "
                + "'Bob':('Bob', 10), "
                + "'Lily':('Lily', 9), "
                + "'Tom':('Tom', 10), "
                + "'Jerry':('Jerry', 13), "
                + "'John':('John', 11);";
        try {
            ResultSet resp = session.execute(insertVertexes);
        } catch (IOErrorException e) {
            e.printStackTrace();
        }

        String insertEdges = "INSERT EDGE like(likeness) VALUES "
                + "'Bob'->'Lily':(80.0), "
                + "'Bob'->'Tom':(70.0), "
                + "'Lily'->'Jerry':(84.0), "
                + "'Tom'->'Jerry':(68.3), "
                + "'Bob'->'John':(97.2);";
        try {
            ResultSet resp = session.execute(insertEdges);
        } catch (IOErrorException e) {
            e.printStackTrace();
        }
        pool.close();
    }
}
