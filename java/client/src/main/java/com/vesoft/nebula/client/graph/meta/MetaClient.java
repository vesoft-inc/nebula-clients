/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.meta;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TCompactProtocol;
import com.facebook.thrift.transport.TSocket;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.HostAddr;
import com.vesoft.nebula.client.graph.exception.ExecuteFailedException;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.meta.ErrorCode;
import com.vesoft.nebula.meta.GetEdgeReq;
import com.vesoft.nebula.meta.GetEdgeResp;
import com.vesoft.nebula.meta.GetPartsAllocReq;
import com.vesoft.nebula.meta.GetPartsAllocResp;
import com.vesoft.nebula.meta.GetSpaceReq;
import com.vesoft.nebula.meta.GetSpaceResp;
import com.vesoft.nebula.meta.GetTagReq;
import com.vesoft.nebula.meta.GetTagResp;
import com.vesoft.nebula.meta.HostItem;
import com.vesoft.nebula.meta.IdName;
import com.vesoft.nebula.meta.ListEdgesReq;
import com.vesoft.nebula.meta.ListEdgesResp;
import com.vesoft.nebula.meta.ListHostsReq;
import com.vesoft.nebula.meta.ListHostsResp;
import com.vesoft.nebula.meta.ListSpacesReq;
import com.vesoft.nebula.meta.ListSpacesResp;
import com.vesoft.nebula.meta.ListTagsReq;
import com.vesoft.nebula.meta.ListTagsResp;
import com.vesoft.nebula.meta.MetaService;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.SpaceItem;
import com.vesoft.nebula.meta.TagItem;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClient extends AbstractMetaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaClient.class);

    public static final int LATEST_SCHEMA_VERSION = -1;

    private static final int DEFAULT_TIMEOUT_MS = 1000;
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 3000;
    private static final int DEFAULT_CONNECTION_RETRY_SIZE = 3;
    private static final int DEFAULT_EXECUTION_RETRY_SIZE = 3;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final MetaInfo metaInfo = new MetaInfo();
    private MetaService.Client client;
    private final List<HostAndPort> addresses;


    public MetaClient(List<HostAndPort> addresses) {
        this(addresses, DEFAULT_CONNECTION_RETRY_SIZE, DEFAULT_EXECUTION_RETRY_SIZE);
    }

    public MetaClient(List<HostAndPort> addresses, int connectionRetry, int executionRetry) {
        this(addresses, DEFAULT_TIMEOUT_MS, connectionRetry, executionRetry);
    }

    public MetaClient(List<HostAndPort> addresses, int timeout,
                      int connectionRetry, int executionRetry) {
        this(addresses, timeout, DEFAULT_CONNECTION_TIMEOUT_MS, connectionRetry, executionRetry);
    }

    public MetaClient(List<HostAndPort> addresses, int timeout,
                      int connectionTimeout, int connectionRetry, int executionRetry) {
        super(addresses, timeout, connectionTimeout, connectionRetry, executionRetry);
        this.addresses = addresses;
    }

    public int connect() throws TException {
        return doConnect();
    }

    /**
     * connect nebula meta server
     *
     * @return 0 -> success, other -> fail
     */
    private int doConnect() throws TException {
        Random random = new Random(System.currentTimeMillis());
        int position = random.nextInt(addresses.size());
        HostAndPort address = addresses.get(position);
        transport = new TSocket(address.getHostText(), address.getPort(), timeout,
                connectionTimeout);
        transport.open();
        protocol = new TCompactProtocol(transport);
        client = new MetaService.Client(protocol);
        freshMetaInfo();
        return ErrorCode.SUCCEEDED;
    }

    /**
     * get all spaces
     *
     * @return spaceName -> spaceId
     */
    public Map<String, Integer> getSpaces() {
        return metaInfo.getSpaceNameMap();
    }

    /**
     * get one space
     *
     * @param spaceName nebula graph space
     * @return SpaceItem
     */
    public SpaceItem getSpace(String spaceName) throws TException {
        GetSpaceReq request = new GetSpaceReq();
        request.setSpace_name(spaceName.getBytes());
        GetSpaceResp response = client.getSpace(request);
        return response.getItem();
    }

    /**
     * get all tags of spaceName
     *
     * @param spaceName nebula graph space
     * @return TagItem list
     */
    public List<TagItem> getTags(String spaceName) throws TException, ExecuteFailedException {

        int spaceID = metaInfo.getSpaceNameMap().get(spaceName);
        ListTagsReq request = new ListTagsReq(spaceID);
        ListTagsResp response;
        try {
            response = client.listTags(request);
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }

        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getTags();
        } else {
            LOGGER.error(String.format("Get tags Error: %s", response.getCode()));
            throw new ExecuteFailedException("Get Tags Error:"
                    + ErrorCode.VALUES_TO_NAMES.get(response.getCode()));
        }
    }


    /**
     * get schema of specific tag
     *
     * @param spaceName nebula graph space
     * @param tagName   nebula tag name
     * @return Schema
     */
    public Schema getTag(String spaceName, String tagName)
            throws TException, ExecuteFailedException {
        GetTagReq request = new GetTagReq();
        int spaceID = metaInfo.getSpaceNameMap().get(spaceName);
        request.setSpace_id(spaceID);
        request.setTag_name(tagName.getBytes());
        request.setVersion(LATEST_SCHEMA_VERSION);
        GetTagResp response;

        try {
            response = client.getTag(request);
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }

        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getSchema();
        } else {
            LOGGER.error(String.format(
                    "Get tag execute Error: %s",
                    ErrorCode.VALUES_TO_NAMES.get(response.getCode())));
            throw new ExecuteFailedException("Get tag execute Error: "
                    + ErrorCode.VALUES_TO_NAMES.get(response.getCode()));
        }
    }


    /**
     * get all edges of specific space
     *
     * @param spaceName nebula graph space
     * @return EdgeItem list
     */
    public List<EdgeItem> getEdges(String spaceName) throws TException, ExecuteFailedException {
        int spaceID = metaInfo.getSpaceNameMap().get(spaceName);
        ListEdgesReq request = new ListEdgesReq(spaceID);
        ListEdgesResp response;
        try {
            response = client.listEdges(request);
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }

        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getEdges();
        } else {
            LOGGER.error(String.format("Get tags Error: %s", response.getCode()));
            throw new ExecuteFailedException("Get Edges Error:"
                    + ErrorCode.VALUES_TO_NAMES.get(response.getCode()));
        }
    }

    /**
     * get schema of specific edgeRow
     *
     * @param spaceName nebula graph space
     * @param edgeName  nebula edgeRow name
     * @return Schema
     */
    public Schema getEdge(String spaceName, String edgeName)
            throws TException, ExecuteFailedException {
        GetEdgeReq request = new GetEdgeReq();
        int spaceID = metaInfo.getSpaceNameMap().get(spaceName);
        request.setSpace_id(spaceID);
        request.setEdge_name(edgeName.getBytes());
        request.setVersion(LATEST_SCHEMA_VERSION);
        GetEdgeResp response;

        try {
            response = client.getEdge(request);
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }

        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getSchema();
        } else {
            LOGGER.error(String.format(
                    "Get Edge execute Error: %s",
                    ErrorCode.VALUES_TO_NAMES.get(response.getCode())));
            throw new ExecuteFailedException(
                    "Get Edge execute Error: "
                            + ErrorCode.VALUES_TO_NAMES.get(response.getCode()));
        }
    }


    /**
     * Get all parts and the address in a space
     * Store in this.parts
     *
     * @param spaceName Nebula space name
     * @return
     */
    public Map<Integer, List<HostAndPort>> getPartsAlloc(String spaceName)
            throws ExecuteFailedException, TException {
        GetPartsAllocReq request = new GetPartsAllocReq();
        int spaceID = metaInfo.getSpaceNameMap().get(spaceName);
        request.setSpace_id(spaceID);

        GetPartsAllocResp response;
        try {
            response = client.getPartsAlloc(request);
        } catch (TException e) {
            LOGGER.error(String.format("Get Parts failed: %s", e.getMessage()));
            throw e;
        }

        if (response.getCode() == ErrorCode.SUCCEEDED) {
            Map<Integer, List<HostAndPort>> addressMap = Maps.newHashMap();
            for (Map.Entry<Integer, List<HostAddr>> entry : response.getParts().entrySet()) {
                List<HostAndPort> addresses = Lists.newLinkedList();
                for (HostAddr address : entry.getValue()) {
                    HostAndPort pair = HostAndPort.fromParts(address.getHost(), address.getPort());
                    addresses.add(pair);
                }
                addressMap.put(entry.getKey(), addresses);
            }
            return addressMap;
        } else {
            LOGGER.error(String.format("Get Parts Error: %s", response.getCode()));
            throw new ExecuteFailedException("Get Parts allocation failed: "
                    + ErrorCode.VALUES_TO_NAMES.get(response.getCode()));
        }
    }


    /**
     * fill the metaInfo
     *
     * @return MetaClient
     */
    public MetaClient freshMetaInfo() {
        for (IdName space : listSpaces()) {
            // space schema
            String spaceName = new String(space.getName());
            int spaceId = space.getId().getSpace_id();
            metaInfo.getSpaceNameMap().put(spaceName, space.getId().getSpace_id());
            metaInfo.getSpacePartLocation().put(spaceName, getPartsLocation(spaceName));

            // tag schema
            Map<Long, TagItem> tags = Maps.newHashMap();
            Map<String, Long> tagsName = Maps.newHashMap();
            Map<Long, String> tagsId = Maps.newHashMap();
            for (TagItem item : listTags(spaceName)) {
                tags.put((long) item.getTag_id(), item);
                tagsName.put(new String(item.getTag_name()), (long) item.getTag_id());
                tagsId.put((long) item.getTag_id(), new String(item.getTag_name()));
            }
            metaInfo.getSpaceTagItems().put(spaceName, tags);
            metaInfo.getTagNameMap().put(spaceName, tagsName);
            metaInfo.getTagIdMap().put(spaceName, tagsId);

            // edgeRow schema
            Map<Long, EdgeItem> edges = Maps.newHashMap();
            Map<String, Long> edgesName = Maps.newHashMap();
            Map<Long, String> edgesId = Maps.newHashMap();
            for (EdgeItem item : listEdges(spaceName)) {
                edges.put((long) item.getEdge_type(), item);
                edgesName.put(new String(item.edge_name), (long) item.getEdge_type());
                edgesId.put((long) item.getEdge_type(), new String(item.edge_name));
            }
            metaInfo.getSpaceEdgeItems().put(spaceName, edges);
            metaInfo.getEdgeNameMap().put(spaceName, edgesName);
            metaInfo.getEdgeIdMap().put(spaceName, edgesId);
        }
        return this;
    }


    /**
     * get all spaces info
     *
     * @return empty list if exception happen
     */
    private List<IdName> listSpaces() {
        ListSpacesReq request = new ListSpacesReq();
        ListSpacesResp response;
        try {
            response = client.listSpaces(request);
        } catch (TException e) {
            LOGGER.error(String.format("List Spaces Error: %s", e.getMessage()));
            return Lists.newLinkedList();
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getSpaces();
        } else {
            LOGGER.error(String.format("List Spaces Error Code: %d", response.getCode()));
            return Lists.newLinkedList();
        }
    }


    /**
     * get all parts and part's location
     *
     * @param spaceName nebula graph space
     * @return empty map if exception happen
     */
    private Map<Integer, List<HostAndPort>> getPartsLocation(String spaceName) {
        Map<Integer, List<HostAndPort>> result;
        try {
            result = getPartsAlloc(spaceName);
        } catch (ExecuteFailedException | TException e) {
            return Maps.newHashMap();
        }
        return result;
    }

    /**
     * get all tags info
     *
     * @param spaceName nebula graph space
     * @return empty list if exception happen
     */
    private List<TagItem> listTags(String spaceName) {
        List<TagItem> tagItems;
        try {
            tagItems = getTags(spaceName);
        } catch (TException | ExecuteFailedException e) {
            return Lists.newLinkedList();
        }
        return tagItems;
    }

    /**
     * get all edges info
     *
     * @param spaceName nebula graph space
     * @return empty list if exception happen
     */
    private List<EdgeItem> listEdges(String spaceName) {
        List<EdgeItem> edgeItems;
        try {
            edgeItems = getEdges(spaceName);
        } catch (TException | ExecuteFailedException e) {
            return Lists.newLinkedList();
        }
        return edgeItems;
    }

    /**
     * check if space exist
     */
    private boolean existSpace(String spaceName) {
        List<IdName> spaces = listSpaces();
        for (IdName space : spaces) {
            if (new String(space.getName()).equals(spaceName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * check if tag exist
     */
    private boolean existTag(String spaceName, String tag) {
        List<TagItem> tags = null;
        try {
            tags = getTags(spaceName);
        } catch (Exception e) {
            LOGGER.error("failed to get tags", e);
            return false;
        }
        for (TagItem item : tags) {
            if (new String(item.getTag_name()).equals(tag)) {
                return true;
            }
        }
        return false;
    }

    /**
     * check if edgeRow exist
     */
    private boolean existEdge(String spaceName, String edgeRow) {
        List<EdgeItem> edges;
        try {
            edges = getEdges(spaceName);
        } catch (Exception e) {
            LOGGER.error("failed to get edges", e);
            return false;
        }
        for (EdgeItem item : edges) {
            if (new String(item.getEdge_name()).equals(edgeRow)) {
                return true;
            }
        }
        return false;
    }


    public MetaInfo getMetaInfo() {
        return metaInfo;
    }


    public HostAndPort getLeader(String spaceName, int part) {

        if (!metaInfo.getSpacePartLocation().containsKey(spaceName)) {
            if (existSpace(spaceName)) {
                lock.writeLock().lock();
                try {
                    freshMetaInfo();
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                throw new IllegalArgumentException("space " + spaceName + " does not exist");
            }
        }

        Map<String, Map<Integer, HostAndPort>> leaders = metaInfo.getLeaders();

        if (!leaders.containsKey(spaceName)) {
            writeLock.lock();
            leaders.put(spaceName, Maps.newConcurrentMap());
            writeLock.unlock();
        }

        if (leaders.get(spaceName).containsKey(part)) {
            HostAndPort leader = null;
            if (leaders.get(spaceName).containsKey(part)) {
                leader = leaders.get(spaceName).get(part);
            }
            return leader;
        }

        readLock.lock();
        if (metaInfo.getSpacePartLocation().containsKey(spaceName)
                && metaInfo.getSpacePartLocation().get(spaceName).containsKey(part)) {
            List<HostAndPort> addresses;
            try {
                addresses = metaInfo.getSpacePartLocation().get(spaceName).get(part);
            } finally {
                lock.readLock().unlock();
            }
            if (addresses != null) {
                Random random = new Random(System.currentTimeMillis());
                int position = random.nextInt(addresses.size());
                HostAndPort leader = addresses.get(position);
                Map<Integer, HostAndPort> partLeader = leaders.get(spaceName);
                writeLock.lock();
                try {
                    partLeader.put(part, leader);
                } finally {
                    writeLock.unlock();
                }
                return leader;
            }
        }
        return null;
    }

    public int getSpaceId(String spaceName) {
        if (!metaInfo.getSpaceNameMap().containsKey(spaceName)) {
            if (existSpace(spaceName)) {
                if (lock.writeLock().tryLock()) {
                    freshMetaInfo();
                }
                lock.writeLock().unlock();
            } else {
                throw new IllegalArgumentException("space " + spaceName + " does not exist");
            }
        }
        int spaceId;
        readLock.lock();
        try {
            spaceId = metaInfo.getSpaceNameMap().get(spaceName);
        } finally {
            readLock.unlock();
        }
        return spaceId;
    }

    public long getTagId(String spaceName, String tagName) {
        if (!metaInfo.getTagNameMap().get(spaceName).containsKey(tagName)) {
            if (existTag(spaceName, tagName)) {
                writeLock.lock();
                try {
                    freshMetaInfo();
                } finally {
                    writeLock.unlock();
                }
            } else {
                throw new IllegalArgumentException(
                        String.format("Tag %s does not exist in space %s", tagName, spaceName));
            }
        }
        long tagId;
        readLock.lock();
        try {
            tagId = metaInfo.getTagNameMap().get(spaceName).get(tagName);
        } finally {
            readLock.unlock();
        }
        return tagId;
    }

    public long getEdgeId(String spaceName, String edgeName) {
        if (!metaInfo.getEdgeNameMap().get(spaceName).containsKey(edgeName)) {
            if (existEdge(spaceName, edgeName)) {
                writeLock.lock();
                try {
                    freshMetaInfo();
                } finally {
                    writeLock.unlock();
                }
            } else {
                throw new IllegalArgumentException(
                        String.format("Edge %s does not exist in space %s", edgeName, spaceName));
            }
        }
        long edgeId;
        readLock.lock();
        try {
            edgeId = metaInfo.getEdgeNameMap().get(spaceName).get(edgeName);
        } finally {
            readLock.unlock();
        }
        return edgeId;
    }

    public List<Integer> getSpaceParts(String spaceName) {
        if (!metaInfo.getSpacePartLocation().containsKey(spaceName)) {
            if (existSpace(spaceName)) {
                writeLock.lock();
                try {
                    freshMetaInfo();
                } finally {
                    writeLock.unlock();
                }
            } else {
                throw new IllegalArgumentException("space " + spaceName + " does not exist");
            }
        }
        Set<Integer> spaceParts;
        readLock.lock();
        try {
            spaceParts = metaInfo.getSpacePartLocation().get(spaceName).keySet();
        } finally {
            readLock.unlock();
        }
        return new ArrayList<Integer>(spaceParts);
    }

    /**
     * cache new leader for part
     *
     * @param spaceName nebula graph space
     * @param part      nebula part
     * @param newLeader nebula part new leader
     */
    public void freshLeader(String spaceName, int part, HostAndPort newLeader) {
        if (!metaInfo.getLeaders().containsKey(spaceName)) {
            getLeader(spaceName, part);
        }

        metaInfo.getLeaders().get(spaceName).put(part, newLeader);
    }


    public Set<HostAndPort> listHosts() {
        ListHostsReq request = new ListHostsReq();
        ListHostsResp resp;
        try {
            resp = client.listHosts(request);
        } catch (TException e) {
            LOGGER.error("listHosts error", e);
            return null;
        }
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (HostItem hostItem : resp.hosts) {
            HostAddr addr = hostItem.getHostAddr();
            hostAndPorts.add(HostAndPort.fromParts(addr.getHost(), addr.getPort()));
        }
        return hostAndPorts;
    }
}
