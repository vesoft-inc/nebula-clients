/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.storage;

import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.storage.StorageClient;
import com.vesoft.nebula.client.graph.storage.data.VertexRow;
import com.vesoft.nebula.client.graph.storage.data.VertexTableView;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResult;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResultIterator;
import java.util.ArrayList;
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

        List<HostAndPort> metaAddress = Arrays.asList(HostAndPort.fromParts(args[0],
                Integer.parseInt(args[1])));
        StorageClient client = new StorageClient(metaAddress);
        try {
            client.connect();
        } catch (Exception e) {
            LOGGER.error("connect error, ", e);
            return;
        }
        scanVertexWithNoColumn(client);
        scanVertexWithAllColumns(client);
        scanVertexWithSpecificColumns(client);
    }

    /**
     * scan vertex with noColumns
     */
    public static void scanVertexWithNoColumn(StorageClient client) {
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put(tag, Arrays.asList("name"));
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(space, tag);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (VertexRow vertexRow : result.getVertices()) {
                    System.out.println(vertexRow.toString());
                }
                for (VertexTableView row : result.getVertexRows()) {
                    System.out.println(row.getVid());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan vertex error", e);
        }
    }

    /**
     * scan vertex with all columns
     */
    public static void scanVertexWithAllColumns(StorageClient client) {
        List<String> returnCols = new ArrayList<>();
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(space, tag, returnCols);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (VertexRow vertexRow : result.getVertices()) {
                    System.out.println(vertexRow.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan vertex error", e);
        }
    }

    /**
     * scan vertex with all columns
     */
    public static void scanVertexWithSpecificColumns(StorageClient client) {
        List<String> returnCols = Arrays.asList("name");
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(space, tag, returnCols);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (VertexRow vertexRow : result.getVertices()) {
                    System.out.println(vertexRow.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("scan vertex error", e);
        }
    }


}
