/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.facebook.thrift.TException;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import com.vesoft.nebula.client.graph.meta.MetaInfo;
import com.vesoft.nebula.client.graph.storage.processor.EdgeProcessor;
import com.vesoft.nebula.client.graph.storage.processor.VertexProcessor;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResultIterator;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResultIterator;
import com.vesoft.nebula.storage.EdgeProp;
import com.vesoft.nebula.storage.ScanEdgeRequest;
import com.vesoft.nebula.storage.ScanVertexRequest;
import com.vesoft.nebula.storage.VertexProp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClient.class);

    private final StorageConnection connection;
    private final StorageConnPool pool;
    private final MetaClient metaClient;
    private final MetaInfo metaInfo;

    public StorageClient(StorageConnPool pool,
                         StorageConnection connection, MetaClient metaClient) {
        this.pool = pool;
        this.connection = connection;
        this.metaClient = metaClient;
        this.metaInfo = metaClient.getMetaInfo();
    }


    /**
     * scan vertex
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               Map<String, List<String>> returnCols)
            throws Exception {
        return scanVertex(spaceName, returnCols, DEFAULT_NO_COLUMNS,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    public ScanVertexResultIterator scanVertex(String spaceName,
                                               int part,
                                               Map<String, List<String>> returnCols) {
        return scanVertex(spaceName, part, returnCols, DEFAULT_NO_COLUMNS,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    public ScanVertexResultIterator scanVertex(String spaceName, Map<String,
            List<String>> returnCols, boolean noColums) throws Exception {
        return scanVertex(spaceName, returnCols, noColums, DEFAULT_LIMIT,
                DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    public ScanVertexResultIterator scanVertex(String spaceName,
                                               int part,
                                               Map<String, List<String>> returnCols,
                                               boolean noColums) {
        return scanVertex(spaceName, part, returnCols, noColums,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }


    public ScanVertexResultIterator scanVertex(String spaceName,
                                               Map<String, List<String>> returnCols,
                                               boolean noColums,
                                               int limit) throws Exception {
        return scanVertex(spaceName, returnCols, noColums,
                limit, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }


    /**
     * scan edge with specific part
     *
     * @param spaceName  nebula graph space
     * @param part       part to scan
     * @param returnCols return cols
     * @param noColumns  if return no col
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               int part,
                                               Map<String, List<String>> returnCols,
                                               boolean noColumns,
                                               int limit,
                                               long startTime,
                                               long endTime) {
        Map<Integer, HostAndPort> partLeaders = Maps.newHashMap();
        HostAndPort leader = getLeader(spaceName, part);
        if (leader == null) {
            throw new IllegalArgumentException("Part " + part + " not found in space " + spaceName);
        }
        partLeaders.put(part, getLeader(spaceName, part));

        List<VertexProp> vertexCols = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : returnCols.entrySet()) {
            long tag = getTagId(spaceName, entry.getKey());
            List<byte[]> props = new ArrayList<>();
            for (String prop : entry.getValue()) {
                props.add(prop.getBytes());
            }
            VertexProp vertexProp = new VertexProp((int) tag, props);
            vertexCols.add(vertexProp);
        }
        ScanVertexRequest request = new ScanVertexRequest();
        request
                .setSpace_id(getSpaceId(spaceName))
                .setReturn_columns(vertexCols)
                .setNo_columns(noColumns)
                .setLimit(limit)
                .setStart_time(startTime)
                .setEnd_time(endTime);


        return doScanVertex(spaceName, partLeaders, request);
    }


    public ScanVertexResultIterator scanVertex(String spaceName,
                                               Map<String, List<String>> returnCols,
                                               boolean noColumns,
                                               int limit,
                                               long startTime,
                                               long endTime) throws Exception {

        Set<Integer> parts = getSpaceParts(spaceName);
        if (parts.isEmpty()) {
            throw new TException("No valid part in space " + spaceName);
        }
        return scanVertex(spaceName, new ArrayList<>(parts),
                returnCols, noColumns, limit, startTime, endTime);
    }


    /**
     * scan vertex with parts
     *
     * @param spaceName  nebula graph space
     * @param parts      parts to scan
     * @param returnCols return cols
     * @param noColumns  if return no col
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    private ScanVertexResultIterator scanVertex(String spaceName,
                                                List<Integer> parts,
                                                Map<String, List<String>> returnCols,
                                                boolean noColumns,
                                                int limit,
                                                long startTime,
                                                long endTime) {

        Map<Integer, HostAndPort> leaders = Maps.newHashMap();
        for (int part : parts) {
            leaders.put(part, getLeader(spaceName, part));
        }

        List<VertexProp> vertexCols = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : returnCols.entrySet()) {
            long tag = getTagId(spaceName, entry.getKey());
            List<byte[]> props = new ArrayList<>();
            for (String prop : entry.getValue()) {
                props.add(prop.getBytes());
            }
            VertexProp vertexProp = new VertexProp((int) tag, props);
            vertexCols.add(vertexProp);
        }
        ScanVertexRequest request = new ScanVertexRequest();
        request
                .setSpace_id(getSpaceId(spaceName))
                .setReturn_columns(vertexCols)
                .setNo_columns(noColumns)
                .setLimit(limit)
                .setStart_time(startTime)
                .setEnd_time(endTime);

        return doScanVertex(spaceName, leaders, request);
    }


    /**
     * do scan vertex
     *
     * @param spaceName   nebula graph space
     * @param partLeaders leaders of all parts
     * @param request     {@link ScanVertexRequest}
     * @return result iterator
     */
    private ScanVertexResultIterator doScanVertex(String spaceName,
                                                  Map<Integer, HostAndPort> partLeaders,
                                                  ScanVertexRequest request) {

        VertexProcessor vertexProcessor = new VertexProcessor(spaceName, metaInfo);

        return new ScanVertexResultIterator.ScanVertexResultBuilder()
                .withPartLeaders(partLeaders)
                .withRequest(request)
                .withProcessor(vertexProcessor)
                .withMetaClient(metaClient)
                .withPool(pool)
                .withSpaceName(spaceName)
                .build();
    }

    /**
     * scan edge
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           Map<String, List<String>> returnCols) throws Exception {
        return scanEdge(spaceName, returnCols, DEFAULT_NO_COLUMNS,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           int part,
                                           Map<String, List<String>> returnCols) {
        return scanEdge(spaceName, part, returnCols, DEFAULT_NO_COLUMNS,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           Map<String, List<String>> returnCols,
                                           boolean noColums) throws Exception {
        return scanEdge(spaceName, returnCols, noColums, DEFAULT_LIMIT,
                DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           int part,
                                           Map<String, List<String>> returnCols,
                                           boolean noColums) {
        return scanEdge(spaceName, part, returnCols, noColums,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }


    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           Map<String, List<String>> returnCols,
                                           boolean noColums,
                                           int limit) throws Exception {
        return scanEdge(spaceName, returnCols, noColums,
                limit, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }


    /**
     * scan edge with specific part
     *
     * @param spaceName  nebula graph space
     * @param part       part to scan
     * @param returnCols return cols
     * @param noColumns  if return no col
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           int part,
                                           Map<String, List<String>> returnCols,
                                           boolean noColumns,
                                           int limit,
                                           long startTime,
                                           long endTime) {
        Map<Integer, HostAndPort> partLeaders = Maps.newHashMap();
        HostAndPort leader = getLeader(spaceName, part);
        if (leader == null) {
            throw new IllegalArgumentException("Part " + part + " not found in space " + spaceName);
        }
        partLeaders.put(part, leader);

        List<EdgeProp> edgeCols = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : returnCols.entrySet()) {
            long edgeId = getEdgeId(spaceName, entry.getKey());
            List<byte[]> props = new ArrayList<>();
            for (String prop : entry.getValue()) {
                props.add(prop.getBytes());
            }
            EdgeProp edgeProp = new EdgeProp((int) edgeId, props);
            edgeCols.add(edgeProp);
        }
        ScanEdgeRequest request = new ScanEdgeRequest();
        request
                .setSpace_id(getSpaceId(spaceName))
                .setReturn_columns(edgeCols)
                .setNo_columns(noColumns)
                .setLimit(limit)
                .setStart_time(startTime)
                .setEnd_time(endTime);


        return doScanEdge(spaceName, partLeaders, request);
    }


    /**
     * scan edge with no specific parts
     *
     * @param spaceName  nebula graph space
     * @param returnCols return cols
     * @param noColumns  if return no col
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           Map<String, List<String>> returnCols,
                                           boolean noColumns,
                                           int limit,
                                           long startTime,
                                           long endTime) throws Exception {
        Set<Integer> parts = getSpaceParts(spaceName);

        if (parts.isEmpty()) {
            throw new TException("No valid part in space " + spaceName);
        }
        return scanEdge(spaceName, new ArrayList<>(parts),
                returnCols, noColumns, limit, startTime, endTime);
    }


    /**
     * scan edge with parts
     *
     * @param spaceName  nebula graph space
     * @param parts      parts to scan
     * @param returnCols return cols
     * @param noColumns  if return no col
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    private ScanEdgeResultIterator scanEdge(String spaceName,
                                            List<Integer> parts,
                                            Map<String, List<String>> returnCols,
                                            boolean noColumns,
                                            int limit,
                                            long startTime,
                                            long endTime) {
        Map<Integer, HostAndPort> leaders = Maps.newHashMap();
        for (int part : parts) {
            leaders.put(part, getLeader(spaceName, part));
        }

        List<EdgeProp> edgeCols = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : returnCols.entrySet()) {
            long edgeId = getEdgeId(spaceName, entry.getKey());
            List<byte[]> props = new ArrayList<>();
            for (String prop : entry.getValue()) {
                props.add(prop.getBytes());
            }
            EdgeProp edgeProp = new EdgeProp((int) edgeId, props);
            edgeCols.add(edgeProp);
        }
        ScanEdgeRequest request = new ScanEdgeRequest();
        request
                .setSpace_id(getSpaceId(spaceName))
                .setReturn_columns(edgeCols)
                .setNo_columns(noColumns)
                .setLimit(limit)
                .setStart_time(startTime)
                .setEnd_time(endTime);

        return doScanEdge(spaceName, leaders, request);
    }

    /**
     * do scan edge
     *
     * @param spaceName   nebula graph space
     * @param partLeaders leaders of all parts
     * @param request     {@link ScanEdgeRequest}
     * @return result iterator
     */
    private ScanEdgeResultIterator doScanEdge(String spaceName,
                                              Map<Integer, HostAndPort> partLeaders,
                                              ScanEdgeRequest request) {
        EdgeProcessor edgeProcessor = new EdgeProcessor(spaceName, metaInfo);
        return new ScanEdgeResultIterator.ScanEdgeResultBuilder()
                .withPartLeaders(partLeaders)
                .withRequest(request)
                .withProcessor(edgeProcessor)
                .withMetaClient(metaClient)
                .withPool(pool)
                .withSpaceName(spaceName)
                .build();
    }


    /**
     * release storage client
     */
    public void close() throws Exception {
        pool.release(connection.getAddress(), connection);
    }


    /**
     * return client's conenction
     *
     * @return StorageConnection
     */
    public StorageConnection getConnection() {
        return this.connection;
    }


    /**
     * get space id from space name
     */
    private int getSpaceId(String spaceName) {
        return metaClient.getSpaceId(spaceName);
    }

    /**
     * get space parts
     */
    private Set<Integer> getSpaceParts(String spaceName) {
        return metaClient.getSpaceParts(spaceName);
    }

    /**
     * get leader of part
     *
     * @param spaceName nebula graph space
     * @param part      nebula part
     * @return HostAndPort
     */
    private HostAndPort getLeader(String spaceName, int part) {
        return metaClient.getLeader(spaceName, part);
    }

    /**
     * get tag id
     */
    private long getTagId(String spaceName, String tagName) {
        return metaClient.getTagId(spaceName, tagName);
    }

    /**
     * get edge id
     */
    private long getEdgeId(String spaceName, String edgeName) {
        return metaClient.getEdgeId(spaceName, edgeName);
    }

    private static final Boolean DEFAULT_NO_COLUMNS = false;
    private static final int DEFAULT_LIMIT = 1;
    private static final long DEFAULT_START_TIME = 0;
    private static final long DEFAULT_END_TIME = Long.MAX_VALUE;
}
