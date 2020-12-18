/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import com.vesoft.nebula.client.graph.meta.MetaInfo;
import com.vesoft.nebula.client.graph.storage.scan.PartScanInfo;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResultIterator;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResultIterator;
import com.vesoft.nebula.storage.EdgeProp;
import com.vesoft.nebula.storage.ScanEdgeRequest;
import com.vesoft.nebula.storage.ScanVertexRequest;
import com.vesoft.nebula.storage.VertexProp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClient.class);

    private final GraphStorageConnection connection;
    private StorageConnPool pool;
    private MetaClient metaClient;
    private MetaInfo metaInfo;
    private final List<HostAndPort> addresses;
    private int timeout = 10000; // ms

    public StorageClient(String ip, int port) {
        this(HostAndPort.fromParts(ip, port));
    }

    public StorageClient(HostAndPort address) {
        this(Arrays.asList(address));
    }

    public StorageClient(HostAndPort address, int timeout) {
        this(Arrays.asList(address), timeout);
    }

    public StorageClient(List<HostAndPort> addresses) {
        this.connection = new GraphStorageConnection();
        this.addresses = addresses;
    }

    public StorageClient(List<HostAndPort> addresses, int timeout) {
        this.connection = new GraphStorageConnection();
        this.addresses = addresses;
        this.timeout = timeout;
    }

    public boolean connect() throws Exception {
        connection.open(addresses.get(0), timeout);
        StoragePoolConfig config = new StoragePoolConfig();
        pool = new StorageConnPool(config, addresses);
        metaClient = pool.getMetaClient();
        metaInfo = metaClient.getMetaInfo();
        return true;
    }


    /**
     * scan vertex of all parts with specific return cols
     */
    public ScanVertexResultIterator scanVertex(String spaceName, String tagName,
                                               List<String> returnCols) throws Exception {

        return scanVertex(spaceName, tagName, returnCols,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan vertex of specific part with specific return cols
     */
    public ScanVertexResultIterator scanVertex(String spaceName, int part, String tagName,
                                               List<String> returnCols) {
        return scanVertex(spaceName, part, tagName, returnCols,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan vertex of all parts with no return cols
     */
    public ScanVertexResultIterator scanVertex(String spaceName, String tagName)
            throws Exception {
        return scanVertex(spaceName, tagName, true,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan vertex of specific part with no return cols
     */
    public ScanVertexResultIterator scanVertex(String spaceName, int part, String tagName) {
        return scanVertex(spaceName, part, tagName, true,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan vertex of all part with specific return cols and limit
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               String tagName,
                                               List<String> returnCols,
                                               int limit) throws Exception {
        return scanVertex(spaceName, tagName, returnCols,
                limit, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan vertex of all part with specific return cols and limit
     */
    public ScanVertexResultIterator scanVertex(String spaceName, String tagName, int limit)
            throws TException {
        return scanVertex(spaceName, tagName, true, limit, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan vertex of specific part
     *
     * @param spaceName  nebula graph space
     * @param part       part to scan
     * @param tagName    nebula tag name
     * @param returnCols return cols
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               int part,
                                               String tagName,
                                               List<String> returnCols,
                                               int limit,
                                               long startTime,
                                               long endTime) {
        assert (returnCols != null);

        return scanVertex(spaceName, Arrays.asList(part), tagName, returnCols, false, limit,
                startTime, endTime);
    }


    /**
     * scan vertex of all parts
     *
     * @param spaceName  nebula graph space
     * @param returnCols return cols
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               String tagName,
                                               List<String> returnCols,
                                               int limit,
                                               long startTime,
                                               long endTime) throws Exception {
        assert (returnCols != null);

        List<Integer> parts = metaClient.getSpaceParts(spaceName);
        if (parts.isEmpty()) {
            throw new TException("No valid part in space " + spaceName);
        }
        return scanVertex(spaceName, parts, tagName,
                returnCols, false, limit, startTime, endTime);
    }


    /**
     * scan vertex of specific part with no return cols
     *
     * @param spaceName nebula graph space
     * @param part      part to scan
     * @param tagName   nebula tag name
     * @param noColumns return no cols
     * @param limit     return data limit
     * @param startTime start time
     * @param endTime   end time
     * @return
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               int part,
                                               String tagName,
                                               boolean noColumns,
                                               int limit,
                                               long startTime,
                                               long endTime) {
        return scanVertex(spaceName, Arrays.asList(part), tagName, new ArrayList<>(), noColumns,
                limit,
                startTime, endTime);
    }


    /**
     * scan vertex of all parts with no return cols
     *
     * @param spaceName nebula graph space
     * @param tagName   nebula tag name
     * @param noColumns return no cols
     * @param limit     return data limit
     * @param startTime start time
     * @param endTime   end time
     * @return
     */
    public ScanVertexResultIterator scanVertex(String spaceName,
                                               String tagName,
                                               boolean noColumns,
                                               int limit,
                                               long startTime,
                                               long endTime) throws TException {
        List<Integer> parts = metaClient.getSpaceParts(spaceName);
        if (parts.isEmpty()) {
            throw new TException("No valid part in space " + spaceName);
        }
        return scanVertex(spaceName, parts, tagName,
                new ArrayList<>(), noColumns, limit, startTime, endTime);
    }


    /**
     * scan vertex with parts
     *
     * @param spaceName  nebula graph space
     * @param parts      parts to scan
     * @param tagName    nebula tag name
     * @param returnCols return cols
     * @param noColumns  if return no col
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    private ScanVertexResultIterator scanVertex(String spaceName,
                                                List<Integer> parts,
                                                String tagName,
                                                List<String> returnCols,
                                                boolean noColumns,
                                                int limit,
                                                long startTime,
                                                long endTime) {

        Set<PartScanInfo> partScanInfoSet = new HashSet<>();
        for (int part : parts) {
            partScanInfoSet.add(new PartScanInfo(part, metaClient.getLeader(spaceName, part)));
        }
        Set<HostAndPort> addrs;
        try {
            addrs = metaClient.listHosts();
        } catch (Exception e) {
            LOGGER.error("list hosts error,", e);
            throw e;
        }

        List<VertexProp> vertexCols = new ArrayList<>();

        long tag = metaClient.getTagId(spaceName, tagName);
        List<byte[]> props = new ArrayList<>();
        for (String prop : returnCols) {
            props.add(prop.getBytes());
        }
        VertexProp vertexProp = new VertexProp((int) tag, props);
        vertexCols.add(vertexProp);

        ScanVertexRequest request = new ScanVertexRequest();
        request
                .setSpace_id(getSpaceId(spaceName))
                .setReturn_columns(vertexCols)
                .setNo_columns(noColumns)
                .setLimit(limit)
                .setStart_time(startTime)
                .setEnd_time(endTime);

        return doScanVertex(spaceName, tagName, partScanInfoSet, request, addrs);
    }


    /**
     * do scan vertex
     *
     * @param spaceName       nebula graph space
     * @param tagName         nebula tag name
     * @param partScanInfoSet leaders of all parts
     * @param request         {@link ScanVertexRequest}
     * @param addrs           storage address set
     * @return result iterator
     */
    private ScanVertexResultIterator doScanVertex(String spaceName,
                                                  String tagName,
                                                  Set<PartScanInfo> partScanInfoSet,
                                                  ScanVertexRequest request,
                                                  Set<HostAndPort> addrs) {
        assert (spaceName != null && tagName != null);

        return new ScanVertexResultIterator.ScanVertexResultBuilder()
                .withMetaClient(metaClient)
                .withPool(pool)
                .withPartScanInfo(partScanInfoSet)
                .withRequest(request)
                .withAddresses(addrs)
                .withSpaceName(spaceName)
                .withTagName(tagName)
                .build();
    }


    /**
     * scan edge of all parts with specific return cols
     */
    public ScanEdgeResultIterator scanEdge(String spaceName, String edgeName,
                                           List<String> returnCols) throws Exception {

        return scanEdge(spaceName, edgeName, returnCols,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan edge of specific part with specific return cols
     */
    public ScanEdgeResultIterator scanEdge(String spaceName, int part, String edgeName,
                                           List<String> returnCols) {

        return scanEdge(spaceName, part, edgeName, returnCols,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan edge of all parts with no return cols
     */
    public ScanEdgeResultIterator scanEdge(String spaceName, String edgeName) throws TException {
        return scanEdge(spaceName, edgeName, true,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan edge of specific part with no return cols
     */
    public ScanEdgeResultIterator scanEdge(String spaceName, int part, String edgeName) {
        return scanEdge(spaceName, part, edgeName, true,
                DEFAULT_LIMIT, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan edge of all part with specific return cols and limit
     */
    public ScanEdgeResultIterator scanEdge(String spaceName, String edgeName,
                                           List<String> returnCols,
                                           int limit) throws Exception {
        return scanEdge(spaceName, edgeName, returnCols,
                limit, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan edge of all part with specific return cols and limit
     */
    public ScanEdgeResultIterator scanEdge(String spaceName, String edgeName, int limit)
            throws Exception {
        return scanEdge(spaceName, edgeName, true, limit, DEFAULT_START_TIME, DEFAULT_END_TIME);
    }

    /**
     * scan edge of specific part
     *
     * @param spaceName  nebula graph space
     * @param part       part to scan
     * @param returnCols return cols
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           int part,
                                           String edgeName,
                                           List<String> returnCols,
                                           int limit,
                                           long startTime,
                                           long endTime) {
        assert (returnCols != null);

        return scanEdge(spaceName, Arrays.asList(part), edgeName, returnCols, false, limit,
                startTime, endTime);
    }


    /**
     * scan edge of all parts
     *
     * @param spaceName  nebula graph space
     * @param edgeName   nebula edge name
     * @param returnCols return cols
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           String edgeName,
                                           List<String> returnCols,
                                           int limit,
                                           long startTime,
                                           long endTime) throws Exception {
        assert (returnCols != null);

        List<Integer> parts = metaClient.getSpaceParts(spaceName);
        if (parts.isEmpty()) {
            throw new TException("No valid part in space " + spaceName);
        }
        return scanEdge(spaceName, parts, edgeName,
                returnCols, false, limit, startTime, endTime);
    }


    /**
     * scan edge of specific part with no return cols
     *
     * @param spaceName nebula graph space
     * @param edgeName  nebula edge name
     * @param part      part to scan
     * @param noColumns return no cols
     * @param limit     return data limit
     * @param startTime start time
     * @param endTime   end time
     * @return
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           int part,
                                           String edgeName,
                                           boolean noColumns,
                                           int limit,
                                           long startTime,
                                           long endTime) {
        return scanEdge(spaceName, Arrays.asList(part), edgeName, new ArrayList<>(), noColumns,
                limit, startTime, endTime);
    }


    /**
     * scan edge of all parts with no return cols
     *
     * @param spaceName nebula graph space
     * @param edgeName  nebula edge name
     * @param noColumns return no cols
     * @param limit     return data limit
     * @param startTime start time
     * @param endTime   end time
     * @return
     */
    public ScanEdgeResultIterator scanEdge(String spaceName,
                                           String edgeName,
                                           boolean noColumns,
                                           int limit,
                                           long startTime,
                                           long endTime) throws TException {

        List<Integer> parts = metaClient.getSpaceParts(spaceName);
        if (parts.isEmpty()) {
            throw new TException("No valid part in space " + spaceName);
        }
        return scanEdge(spaceName, parts, edgeName,
                new ArrayList<>(), noColumns, limit, startTime, endTime);
    }


    /**
     * scan edge with parts
     *
     * @param spaceName  nebula graph space
     * @param parts      parts to scan
     * @param edgeName   nebula edge name
     * @param returnCols return cols
     * @param noColumns  if return no col, if noColumns is true, returnCols is useless
     * @param limit      return data limit
     * @param startTime  start time
     * @param endTime    end time
     * @return
     */
    private ScanEdgeResultIterator scanEdge(String spaceName,
                                            List<Integer> parts,
                                            String edgeName,
                                            List<String> returnCols,
                                            boolean noColumns,
                                            int limit,
                                            long startTime,
                                            long endTime) {

        Set<PartScanInfo> partScanInfoSet = new HashSet<>();
        for (int part : parts) {
            partScanInfoSet.add(new PartScanInfo(part, metaClient.getLeader(spaceName, part)));
        }
        Set<HostAndPort> addrs;
        try {
            addrs = metaClient.listHosts();
        } catch (Exception e) {
            LOGGER.error("list hosts error,", e);
            throw e;
        }

        List<EdgeProp> edgeCols = new ArrayList<>();

        long edgeId = getEdgeId(spaceName, edgeName);
        List<byte[]> props = new ArrayList<>();
        for (String prop : returnCols) {
            props.add(prop.getBytes());
        }
        EdgeProp edgeProp = new EdgeProp((int) edgeId, props);
        edgeCols.add(edgeProp);

        ScanEdgeRequest request = new ScanEdgeRequest();
        request
                .setSpace_id(getSpaceId(spaceName))
                .setReturn_columns(edgeCols)
                .setNo_columns(noColumns)
                .setLimit(limit)
                .setStart_time(startTime)
                .setEnd_time(endTime);

        return doScanEdge(spaceName, edgeName, partScanInfoSet, request, addrs);
    }


    /**
     * do scan edge
     *
     * @param spaceName       nebula graph space
     * @param edgeName        nebula edge name
     * @param partScanInfoSet leaders of all parts
     * @param request         {@link ScanVertexRequest}
     * @param addrs           storage server list
     * @return result iterator
     */
    private ScanEdgeResultIterator doScanEdge(String spaceName, String edgeName,
                                              Set<PartScanInfo> partScanInfoSet,
                                              ScanEdgeRequest request, Set<HostAndPort> addrs) {
        assert (spaceName != null && edgeName != null && addrs != null);

        return new ScanEdgeResultIterator.ScanEdgeResultBuilder()
                .withMetaClient(metaClient)
                .withPool(pool)
                .withPartScanInfo(partScanInfoSet)
                .withRequest(request)
                .withAddresses(addrs)
                .withSpaceName(spaceName)
                .withEdgeName(edgeName)
                .build();
    }


    /**
     * release storage client
     */
    public void close() {
        pool.close();
        connection.close();
    }


    /**
     * return client's connection
     *
     * @return StorageConnection
     */
    public GraphStorageConnection getConnection() {
        return this.connection;
    }


    /**
     * get space id from space name
     */
    private int getSpaceId(String spaceName) {
        return metaClient.getSpaceId(spaceName);
    }

    /**
     * get edge id
     */
    private long getEdgeId(String spaceName, String edgeName) {
        return metaClient.getEdgeId(spaceName, edgeName);
    }

    private static final int DEFAULT_LIMIT = 1000;
    private static final long DEFAULT_START_TIME = 0;
    private static final long DEFAULT_END_TIME = Long.MAX_VALUE;
}
