/*
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.general;

import com.facebook.thrift.TException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.HostAddr;
import com.vesoft.nebula.KeyValue;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import com.vesoft.nebula.client.graph.meta.MetaInfo;
import com.vesoft.nebula.client.graph.storage.StoragePoolConfig;
import com.vesoft.nebula.storage.ErrorCode;
import com.vesoft.nebula.storage.ExecResponse;
import com.vesoft.nebula.storage.KVGetRequest;
import com.vesoft.nebula.storage.KVGetResponse;
import com.vesoft.nebula.storage.KVPutRequest;
import com.vesoft.nebula.storage.KVRemoveRequest;
import com.vesoft.nebula.storage.PartitionResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.codec.digest.MurmurHash2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * storage client for operater put/get/remove
 * GeneralStorageClient is not thread safe. not support for nebula v2.0.0 yet
 */
public class GeneralStorageClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralStorageClient.class);

    private GeneralStorageConnection connection;
    private MetaInfo metaInfo;
    private MetaClient metaClient;
    private final List<HostAndPort> addresses;
    private ExecutorService threadPool;
    private int timeout = 10000; // ms
    private final int parallel = 10;

    private final Map<HostAndPort, GeneralStorageConnection> leaderConnections = Maps.newHashMap();

    public GeneralStorageClient(String ip, int port) {
        this(HostAndPort.fromParts(ip, port));
    }

    public GeneralStorageClient(HostAndPort address) {
        this(Arrays.asList(address));
    }

    public GeneralStorageClient(HostAndPort address, int timeout) {
        this(Arrays.asList(address), timeout);
    }

    public GeneralStorageClient(List<HostAndPort> addresses) {
        this.connection = new GeneralStorageConnection();
        this.addresses = addresses;
    }

    public GeneralStorageClient(List<HostAndPort> addresses, int timeout) {
        this.connection = new GeneralStorageConnection();
        this.addresses = addresses;
        this.timeout = timeout;
    }

    public boolean connect() throws Exception {
        connection.open(addresses.get(0), timeout);
        StoragePoolConfig config = new StoragePoolConfig();
        metaClient = new MetaClient(addresses);
        metaInfo = metaClient.getMetaInfo();
        threadPool = Executors.newFixedThreadPool(parallel);
        return true;
    }

    /**
     * Put key-value into nebula
     *
     * @param spaceName nebula graph space
     * @param key       nebula key
     * @param value     nebula value
     * @return boolean
     */
    public boolean put(String spaceName, String key, String value) throws Exception {
        int spaceId = metaInfo.getSpaceNameMap().get(spaceName);
        int part = keyToPartId(spaceName, key);

        Map<Integer, List<KeyValue>> parts = Maps.newHashMap();
        List<KeyValue> pairs = Lists.newArrayList(new KeyValue(key.getBytes(), value.getBytes()));
        parts.put(part, pairs);
        KVPutRequest request = new KVPutRequest(spaceId, parts);

        HostAndPort leader = getLeader(spaceName, part);
        return doPut(spaceName, leader, request);
    }


    /**
     * Put multi key-value into nebula
     *
     * @param spaceName nebula graph space
     * @param kvs       key-value pairs
     * @return boolean
     */
    public boolean put(String spaceName, Map<String, String> kvs) {
        int spaceId = metaInfo.getSpaceNameMap().get(spaceName);
        Map<Integer, List<KeyValue>> partKeyValues = Maps.newHashMap();
        for (Map.Entry<String, String> kv : kvs.entrySet()) {
            int part = keyToPartId(spaceName, kv.getKey());
            KeyValue keyValue = new KeyValue(kv.getKey().getBytes(), kv.getValue().getBytes());
            if (!partKeyValues.containsKey(part)) {
                partKeyValues.put(part, new ArrayList<KeyValue>());
            }
            partKeyValues.get(part).add(keyValue);
        }

        Map<HostAndPort, KVPutRequest> requests = Maps.newHashMap();
        for (Map.Entry<Integer, List<KeyValue>> partKeyValue : partKeyValues.entrySet()) {
            int part = partKeyValue.getKey();
            HostAndPort leader = getLeader(spaceName, part);

            if (!requests.containsKey(leader)) {
                KVPutRequest request = new KVPutRequest();
                request.setSpace_id(spaceId);
                Map<Integer, List<KeyValue>> parts = Maps.newHashMap();
                parts.put(part, partKeyValue.getValue());
                request.setParts(parts);
                requests.put(leader, request);
            } else {
                KVPutRequest request = requests.get(leader);
                if (request.getParts().containsKey(part)) {
                    request.getParts().get(part).addAll(partKeyValue.getValue());
                } else {
                    request.getParts().put(part, partKeyValue.getValue());
                }
            }
        }

        final CountDownLatch countDownLatch = new CountDownLatch(partKeyValues.size());
        final List<Boolean> responses = Collections.synchronizedList(
                new ArrayList<>(partKeyValues.size()));

        for (final Map.Entry<HostAndPort, KVPutRequest> entry : requests.entrySet()) {
            threadPool.submit(() -> {
                if (doPut(spaceName, entry.getKey(), entry.getValue())) {
                    responses.add(true);
                } else {
                    responses.add(false);
                }
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException interruptedE) {
            LOGGER.error("Put interrupted:", interruptedE);
            return false;
        }

        for (Boolean resp : responses) {
            if (!resp) {
                return false;
            }
        }

        return true;
    }


    /**
     * wrap request and execute put
     *
     * @param spaceName nebula graph space
     * @param leader    leader for part
     * @param request   PutRequest
     * @return boolean
     */
    private boolean doPut(String spaceName, HostAndPort leader, KVPutRequest request) {
        // there is at least one retry for leader change.
        int retry = 0;
        while (retry++ != metaInfo.getConnectionRetry()) {

            try {
                connection = getConnection(leader);
                if (connection == null) {
                    LOGGER.error("connection is in usage");
                    return false;
                }
                connection.setBusy();
            } catch (Exception e) {
                LOGGER.error("Connect error: ", e);
                return false;
            }

            try {
                ExecResponse result = connection.put(request);
                if (isSuccessfully(result)) {
                    connection.release();
                    return true;
                }
                for (PartitionResult partResult : result.getResult().getFailed_parts()) {
                    if (partResult.code == ErrorCode.E_LEADER_CHANGED) {
                        freshLeader(spaceName, partResult.part_id, partResult.leader);
                    } else {
                        LOGGER.error(String.format("Put failed, error code=%d", partResult.code));
                    }
                }
            } catch (TException e) {
                LOGGER.error("Put Failed:", e);
                connection.release();
                return false;
            }
        }
        connection.release();
        return false;
    }


    /**
     * get value according to spaceName and key
     *
     * @param spaceName nebula graph space
     * @param key       nebula key
     * @return String value
     */
    public String get(String spaceName, String key) throws TException {
        int spaceId = metaClient.getSpaceId(spaceName);
        int part = keyToPartId(spaceName, key);
        HostAndPort leader = getLeader(spaceName, part);

        if (leader == null) {
            throw new TException(String
                    .format("no leader for space %s and key %s", spaceName, key));
        }

        Map<Integer, List<byte[]>> parts = Maps.newHashMap();
        parts.put(part, Arrays.asList(key.getBytes()));
        KVGetRequest request = new KVGetRequest(spaceId, parts, false);

        Map<byte[], byte[]> result = doGet(spaceName, leader, request);
        if (result.containsKey(key.getBytes())) {
            return new String(result.get(key.getBytes()));
        } else {
            return null;
        }
    }


    /**
     * get key-values result according to spaceName and keys
     *
     * @param spaceName nebula graph space
     * @param keys      nebula keys
     * @return
     */
    public Map<String, String> get(String spaceName, List<String> keys)
            throws InterruptedException {
        return get(spaceName, keys, false);
    }


    /**
     * get key-values result according to spaceName and keys
     *
     * @param spaceName    nebula graph space
     * @param keys         nebula keys
     * @param returnPartly return successful partly result when error happen
     * @return
     */
    public Map<String, String> get(String spaceName, List<String> keys,
                                   boolean returnPartly) throws InterruptedException {
        int spaceId = metaInfo.getSpaceNameMap().get(spaceName);
        Map<Integer, List<String>> partKeys = Maps.newHashMap();
        for (String key : keys) {
            int part = keyToPartId(spaceName, key);
            if (!partKeys.containsKey(part)) {
                partKeys.put(part, new ArrayList<>());
            }
            partKeys.get(part).add(key);
        }

        Map<HostAndPort, KVGetRequest> requests = Maps.newHashMap();
        for (Map.Entry<Integer, List<String>> partKey : partKeys.entrySet()) {
            int part = partKey.getKey();
            HostAndPort leader = getLeader(spaceName, part);

            List<byte[]> byteKeys = new ArrayList<>();
            for (String key : partKey.getValue()) {
                byteKeys.add(key.getBytes());
            }
            if (!requests.containsKey(leader)) {
                Map<Integer, List<byte[]>> parts = Maps.newHashMap();
                parts.put(part, byteKeys);
                KVGetRequest request = new KVGetRequest(spaceId, parts, returnPartly);
                requests.put(leader, request);
            } else {
                KVGetRequest request = requests.get(leader);
                if (!request.getParts().containsKey(part)) {
                    request.getParts().put(part, byteKeys);
                } else {
                    request.getParts().get(part).addAll(byteKeys);
                }
            }
        }

        final CountDownLatch countDownLatch = new CountDownLatch(partKeys.size());
        final List<Map<byte[], byte[]>> responses = Collections.synchronizedList(
                new ArrayList<>(partKeys.size()));
        for (final Map.Entry<HostAndPort, KVGetRequest> entry : requests.entrySet()) {
            threadPool.submit(() -> {
                try {
                    responses.add(doGet(spaceName, entry.getKey(), entry.getValue()));
                } catch (TException e) {
                    LOGGER.error("doGet error, ", e);
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException interruptedE) {
            LOGGER.error("Get interrupted, ", interruptedE);
            throw interruptedE;
        }

        Map<String, String> result = new HashMap<>();
        for (Map<byte[], byte[]> response : responses) {
            for (Map.Entry<byte[], byte[]> kv : response.entrySet()) {
                result.put(new String(kv.getKey()), new String(kv.getValue()));
            }
        }
        return result;
    }


    /**
     * warp result and execute get
     *
     * @param spaceName nebula grpah space
     * @param leader    nebula part leader
     * @param request   KVGetRequest
     * @return Map
     */
    private Map<byte[], byte[]> doGet(String spaceName,
                                      HostAndPort leader, KVGetRequest request) throws TException {

        int retry = 0;
        while (retry++ != metaInfo.getConnectionRetry()) {
            try {
                connection = getConnection(leader);
                if (connection == null) {
                    LOGGER.error("connection is in usage");
                    throw new TException("connection is in usage");
                }
            } catch (Exception e) {
                LOGGER.error("Connect error: ", e);
                throw new TException("Connect failed, ", e);
            }
            connection.setBusy();

            try {
                KVGetResponse result = connection.get(request);
                if (isSuccessfully(result)) {
                    return result.getKey_values();
                }
                for (PartitionResult partResult : result.getResult().getFailed_parts()) {
                    if (partResult.code == ErrorCode.E_LEADER_CHANGED) {
                        freshLeader(spaceName, partResult.part_id, partResult.leader);
                    } else {
                        LOGGER.error(String.format("Put failed, error code=%d", partResult.code));
                        connection.release();
                    }
                }
            } catch (TException e) {
                LOGGER.error("Get failed, ", e);
                throw e;
            }
        }
        return Maps.newHashMap();
    }


    /**
     * remove key from nebula
     *
     * @param spaceName nebula graph space
     * @param key       nebula key
     * @return boolean
     */
    public boolean remove(String spaceName, String key) throws TException {
        int spaceId = metaInfo.getSpaceNameMap().get(spaceName);
        int part = keyToPartId(spaceName, key);

        Map<Integer, List<byte[]>> parts = Maps.newHashMap();
        parts.put(part, Arrays.asList(key.getBytes()));
        KVRemoveRequest request = new KVRemoveRequest(spaceId, parts);

        HostAndPort leader = getLeader(spaceName, part);
        return doRemove(spaceName, leader, request);
    }


    /**
     * remove keys from nebula
     *
     * @param spaceName nebula graph space
     * @param keys      nebula key
     * @return boolean
     */
    public boolean remove(String spaceName, List<String> keys)
            throws InterruptedException {
        int spaceId = metaInfo.getSpaceNameMap().get(spaceName);
        Map<Integer, List<String>> partKeys = Maps.newHashMap();
        for (String key : keys) {
            int part = keyToPartId(spaceName, key);
            if (!partKeys.containsKey(part)) {
                partKeys.put(part, new ArrayList<>());
            }
            partKeys.get(part).add(key);
        }

        Map<HostAndPort, KVRemoveRequest> requests = Maps.newHashMap();
        for (Map.Entry<Integer, List<String>> partKey : partKeys.entrySet()) {
            int part = partKey.getKey();
            HostAndPort leader = getLeader(spaceName, part);

            List<byte[]> byteKeys = new ArrayList<>();
            for (String key : partKey.getValue()) {
                byteKeys.add(key.getBytes());
            }
            if (!requests.containsKey(leader)) {
                Map<Integer, List<byte[]>> parts = Maps.newHashMap();
                parts.put(part, byteKeys);
                KVRemoveRequest request = new KVRemoveRequest(spaceId, parts);
                requests.put(leader, request);
            } else {
                KVRemoveRequest request = requests.get(leader);
                if (!request.getParts().containsKey(part)) {
                    request.getParts().put(part, byteKeys);
                } else {
                    request.getParts().get(part).addAll(byteKeys);
                }
            }
        }

        final CountDownLatch countDownLatch = new CountDownLatch(partKeys.size());
        final List<Boolean> responses = Collections.synchronizedList(
                new ArrayList<>(partKeys.size()));
        for (final Map.Entry<HostAndPort, KVRemoveRequest> entry : requests.entrySet()) {
            threadPool.submit(() -> {
                try {
                    responses.add(doRemove(spaceName, entry.getKey(), entry.getValue()));
                } catch (TException e) {
                    LOGGER.error("doRemove error, ", e);
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException interruptedE) {
            LOGGER.error("Get interrupted, ", interruptedE);
            throw interruptedE;
        }

        for (Boolean response : responses) {
            if (!response) {
                return false;
            }
        }
        return true;
    }


    /**
     * warp result and execute remove
     *
     * @param spaceName nebula graph space
     * @param leader    nebula part leader
     * @param request   KVRemoveRequest
     * @return boolean
     */
    private boolean doRemove(String spaceName, HostAndPort leader, KVRemoveRequest request)
            throws TException {
        int retry = 0;
        while (retry++ != metaInfo.getConnectionRetry()) {
            try {
                connection = getConnection(leader);
                if (connection == null) {
                    LOGGER.error("Connect is in usage");
                    throw new TException("Connection is in usage");
                }
            } catch (Exception e) {
                LOGGER.error("Connect error: ", e);
                throw new TException("Connect error,", e);
            }

            connection.setBusy();
            try {
                ExecResponse result = connection.remove(request);
                if (isSuccessfully(result)) {
                    connection.release();
                    return true;
                }
                for (PartitionResult partResult : result.getResult().getFailed_parts()) {
                    if (partResult.code == ErrorCode.E_LEADER_CHANGED) {
                        freshLeader(spaceName, partResult.part_id, partResult.leader);
                    } else {
                        LOGGER.error(String.format("Remove failed, error code=%d",
                                partResult.code));
                    }
                }
            } catch (TException e) {
                LOGGER.error("Put Failed:", e);
                connection.release();
                return false;
            }
        }
        connection.release();
        return false;
    }


    /**
     * get part id by spaceName and key
     *
     * @param spaceName nebula graph space
     * @param key       nebula key
     * @return part id
     */
    private int keyToPartId(String spaceName, String key) {
        if (!metaInfo.getSpacePartLocation().containsKey(spaceName)) {
            LOGGER.error("Invalid part of " + spaceName);
            return -1;
        }
        int partNum = metaInfo.getSpacePartLocation().get(spaceName).size();
        if (partNum <= 0) {
            return -1;
        }
        long hashValue = Long.parseUnsignedLong(Long.toUnsignedString(hash(key)));
        return (int) (Math.floorMod(hashValue, partNum) + 1);
    }

    /**
     * get leader of part
     *
     * @param spaceName nebula graph space
     * @param part      nebula part
     * @return HostAndPort
     */
    private HostAndPort getLeader(String spaceName, int part) {
        Map<String, Map<Integer, HostAndPort>> leaders = metaInfo.getLeaders();
        if (!leaders.containsKey(spaceName)) {
            leaders.put(spaceName, Maps.newConcurrentMap());
        }

        if (leaders.get(spaceName).containsKey(part)) {
            return leaders.get(spaceName).get(part);
        } else {
            if (metaInfo.getSpacePartLocation().containsKey(spaceName)
                    && metaInfo.getSpacePartLocation().get(spaceName).containsKey(part)) {
                List<HostAndPort> address = metaInfo.getSpacePartLocation()
                        .get(spaceName)
                        .get(part);
                if (address != null) {
                    Random random = new Random(System.currentTimeMillis());
                    int position = random.nextInt(address.size());
                    HostAndPort leader = address.get(position);
                    leaders.get(spaceName).put(part, leader);
                    return leader;
                }
            }
            return null;
        }
    }

    /**
     * cache new leader for part
     *
     * @param spaceName nebula graph space
     * @param part      nebula part
     * @param leader    nebula part leader
     */
    private void freshLeader(String spaceName, int part, HostAddr leader) {
        HostAndPort newLeader = HostAndPort.fromParts(leader.getHost(), leader.getPort());
        metaClient.freshLeader(spaceName, part, newLeader);
    }

    /**
     * release storage client
     */
    public void close() throws Exception {
        for (Map.Entry<HostAndPort, GeneralStorageConnection> entry :
                leaderConnections.entrySet()) {
            entry.getValue().close();
        }
        leaderConnections.clear();
        connection.close();
    }


    /**
     * hash key
     *
     * @param key String key
     * @return long
     */
    private long hash(String key) {
        return MurmurHash2.hash64(key);
    }


    /**
     * Check the exec response is successfully
     */
    private boolean isSuccessfully(ExecResponse response) {
        return response.result.failed_parts.size() == 0;
    }

    /**
     * Check the get response is successfully
     */
    private boolean isSuccessfully(KVGetResponse response) {
        return response.result.failed_parts.size() == 0;
    }

    /**
     * return client's conenction
     *
     * @return StorageConnection
     */
    private GeneralStorageConnection getConnection(HostAndPort leader) throws Exception {
        if (!leaderConnections.containsKey(leader)) {
            GeneralStorageConnection connection = new GeneralStorageConnection();
            connection.open(leader, timeout);
            leaderConnections.put(leader, connection);
        }

        GeneralStorageConnection conn = leaderConnections.get(leader);
        if (conn.isBusy()) {
            return null;
        }
        return conn;
    }
}
