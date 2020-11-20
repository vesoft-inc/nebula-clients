/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.storage.data.Edge;
import com.vesoft.nebula.client.graph.storage.data.Vertex;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResult;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResultIterator;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResult;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResultIterator;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;

public class StorageClientTest extends TestCase {

    private StorageConnPool pool;
    private final String spaceName = "test1";
    private final String ip = "127.0.0.1";


    /**
     * init pool
     */
    public void setUp() throws Exception {
        MockNebulaGraph.createGraph();
        List<HostAndPort> address = Arrays.asList(
                HostAndPort.fromParts(ip, 45500),
                HostAndPort.fromParts(ip, 45501),
                HostAndPort.fromParts(ip, 45502)
        );
        NebulaPoolConfig config = new NebulaPoolConfig();
        pool = new StorageConnPool(config, address);
    }

    public void tearDown() throws Exception {
        pool.close();
    }

    private StorageClient getClient() {
        HostAndPort addr = HostAndPort.fromParts(ip, 45500);
        StorageClient client = null;
        try {
            client = pool.getStorageClient(addr);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        return client;
    }

    /**
     * test scan vertex with noColumns
     */
    public void testScanVertexWithNoColumn() {
        StorageClient client = getClient();
        // test scan specific cols
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put("person", Arrays.asList("name"));
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(spaceName, returnCols, true);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Vertex vertex : result.getAllVertices()) {
                    System.out.println(vertex.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * test scan vertex with all columns
     */
    public void testScanVertexWithAllColumns() {
        StorageClient client = getClient();
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put("person", Arrays.asList());
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(spaceName, returnCols, false);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Vertex vertex : result.getAllVertices()) {
                    System.out.println(vertex.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


    /**
     * test scan vertex with all columns
     */
    public void testScanVertexWithSpecificColumns() {
        StorageClient client = getClient();
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put("person", Arrays.asList("name"));
        ScanVertexResultIterator iterator;
        try {
            iterator = client.scanVertex(spaceName, returnCols, false);
            ScanVertexResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Vertex vertex : result.getAllVertices()) {
                    System.out.println(vertex.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * test scan edge with no columns
     */
    public void testScanEdgeWithNoColumns() {
        StorageClient client = getClient();
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put("friend", Arrays.asList("duration"));
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(spaceName, returnCols, true);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Edge edge : result.getAllEdges()) {
                    assert (edge.getEdgeType().getProps().size() == 0);
                    System.out.println(edge.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * test scan edge with all columns
     */
    public void testScanEdgeWithAllColumns() {
        StorageClient client = getClient();
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put("friend", Arrays.asList());
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(spaceName, returnCols, false);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Edge edge : result.getAllEdges()) {
                    assert (edge.getEdgeType().getProps().size() == 3);
                    System.out.println(edge.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * test scan edge with specific columns
     */
    public void testScanEdgeWithSpecificColumns() {
        StorageClient client = getClient();
        Map<String, List<String>> returnCols = Maps.newHashMap();
        returnCols.put("friend", Arrays.asList("duration", "relation"));
        ScanEdgeResultIterator iterator;
        try {
            iterator = client.scanEdge(spaceName, returnCols, false);
            ScanEdgeResult result;
            while (iterator.hasNext()) {
                result = iterator.next();
                for (Edge edge : result.getAllEdges()) {
                    assert (edge.getEdgeType().getProps().size() == 2);
                    assert (edge.getEdgeType().getRank() == 0);
                    System.out.println(edge.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


    public void testClose() {
        pool.close();
        assert (pool.getNumActive(HostAndPort.fromParts(ip, 45509)) == 0);

    }
}