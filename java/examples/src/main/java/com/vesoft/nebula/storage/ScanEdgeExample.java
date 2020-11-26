/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.storage;

import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.storage.StorageClient;
import com.vesoft.nebula.client.graph.storage.data.EdgeRow;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResult;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResultIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanEdgeExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanEdgeExample.class);

    static String space = "test";
    static String edge = "like";

    public static void main(String[] args) {

        List<HostAndPort> metaAddress = Arrays.asList(HostAndPort.fromParts(args[0],
                Integer.valueOf(args[1])));
        StorageClient client = new StorageClient(metaAddress);
        try {
            client.connect();
        } catch (Exception e) {
            LOGGER.error("connect error, ", e);
            return;
        }

        scanEdgeWithAllColumns(client);
        scanEdgeWithNoColumns(client);
        scanEdgeWithSpecificColumns(client);
    }

    /**
     * scan edge with no columns
     */
    public static void scanEdgeWithNoColumns(StorageClient client) {
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(space, edge);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (EdgeRow edgeRow : result.getEdges()) {
                    assert (edgeRow.getProps().size() == 0);
                    System.out.println(edgeRow.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan edge error,", e);
        }
    }

    /**
     * scan edge with all columns
     */
    public static void scanEdgeWithAllColumns(StorageClient client) {
        List<String> returnCols = new ArrayList<>();
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(space, edge, returnCols);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (EdgeRow edgeRow : result.getEdges()) {
                    assert (edgeRow.getProps().size() == 3);
                    System.out.println(edgeRow.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan edge error,", e);
        }
    }

    /**
     * scan edge with specific columns
     */
    public static void scanEdgeWithSpecificColumns(StorageClient client) {
        List<String> returnCols = Arrays.asList("likeness");
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(space, edge, returnCols);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (EdgeRow edgeRow : result.getEdges()) {
                    assert (edgeRow.getProps().size() == 2);
                    assert (edgeRow.getRank() == 0);
                    System.out.println(edgeRow.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan edge error,", e);
        }
    }


}
