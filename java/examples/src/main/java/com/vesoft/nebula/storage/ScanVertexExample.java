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
import com.vesoft.nebula.client.graph.storage.data.Vertex;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResult;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResultIterator;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanVertexExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanVertexExample.class);

    static String space = "test";
    static String tag = "person";

    public static void main(String[] args) {
        NebulaPoolConfig config = new NebulaPoolConfig();
        List<HostAndPort> metaAddress = Arrays.asList(HostAndPort.fromParts(args[0],
                Integer.parseInt(args[1])));
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

        testScanVertexWithNoColumn(client);
        testScanVertexWithAllColumns(client);
        testScanVertexWithSpecificColumns(client);
        pool.close();
    }

    /**
     * test scan vertex with noColumns
     */
    public static void testScanVertexWithNoColumn(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList("name"));
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(space, returnCols, true);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Vertex vertex : result.getAllVertices()) {
                    System.out.println(vertex.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan vertex error", e);
        }
    }

    /**
     * test scan vertex with all columns
     */
    public static void testScanVertexWithAllColumns(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList());
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(space, returnCols, false);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Vertex vertex : result.getAllVertices()) {
                    System.out.println(vertex.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan vertex error", e);
        }
    }

    /**
     * test scan vertex with all columns
     */
    public static void testScanVertexWithSpecificColumns(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList("name"));
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(space, returnCols, false);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Vertex vertex : result.getAllVertices()) {
                    System.out.println(vertex.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan vertex error", e);
        }
    }


}
