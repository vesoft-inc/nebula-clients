/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.storage;

import com.facebook.thrift.TException;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.storage.StorageClient;
import com.vesoft.nebula.client.graph.storage.StorageConnPool;
import com.vesoft.nebula.client.graph.storage.data.Edge;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResult;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResultIterator;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanEdgeExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanEdgeExample.class);

    static String space = "test";
    static String tag = "friend";

    public static void main(String[] args) {
        NebulaPoolConfig config = new NebulaPoolConfig();
        List<HostAndPort> metaAddress = Arrays.asList(HostAndPort.fromParts(args[0],
                Integer.valueOf(args[1])));
        StorageConnPool pool;
        try {
            pool = new StorageConnPool(config, metaAddress);
        } catch (TException e) {
            LOGGER.error("failed to get pool,", e);
            return;
        }
        StorageClient client;
        try {
            client = pool.getStorageClient();
        } catch (Exception e) {
            LOGGER.error("failed to get client,", e);
            return;
        }

        testScanEdgeWithAllColumns(client);
        testScanEdgeWithNoColumns(client);
        testScanEdgeWithSpecificColumns(client);
        pool.close();
    }

    /**
     * test scan edge with no columns
     */
    public static void testScanEdgeWithNoColumns(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList("duration"));
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(space, returnCols, true);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Edge edge : result.getAllEdges()) {
                    assert (edge.getEdgeType().getProps().size() == 0);
                    System.out.println(edge.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan edge error,", e);
        }
    }

    /**
     * test scan edge with all columns
     */
    public static void testScanEdgeWithAllColumns(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList());
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(space, returnCols, false);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Edge edge : result.getAllEdges()) {
                    assert (edge.getEdgeType().getProps().size() == 3);
                    System.out.println(edge.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan edge error,", e);
        }
    }

    /**
     * test scan edge with specific columns
     */
    public static void testScanEdgeWithSpecificColumns(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList("duration", "relation"));
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(space, returnCols, false);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Edge edge : result.getAllEdges()) {
                    assert (edge.getEdgeType().getProps().size() == 2);
                    assert (edge.getRank() == 0);
                    System.out.println(edge.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan edge error,", e);
        }
    }


}
