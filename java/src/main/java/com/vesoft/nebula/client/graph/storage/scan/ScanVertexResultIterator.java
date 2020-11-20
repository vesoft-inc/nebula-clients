/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import com.vesoft.nebula.client.graph.storage.StorageConnPool;
import com.vesoft.nebula.client.graph.storage.StorageConnection;
import com.vesoft.nebula.client.graph.storage.processor.VertexProcessor;
import com.vesoft.nebula.storage.ErrorCode;
import com.vesoft.nebula.storage.PartitionResult;
import com.vesoft.nebula.storage.ScanVertexRequest;
import com.vesoft.nebula.storage.ScanVertexResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScanVertexResult's iterator
 */
public class ScanVertexResultIterator extends ScanResultIterator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanVertexResultIterator.class);

    private final ScanVertexRequest request;
    private final VertexProcessor processor;

    private ScanVertexResultIterator(MetaClient metaClient, String spaceName, StorageConnPool pool,
                                     Map<Integer, HostAndPort> partLeaders,
                                     ScanVertexRequest request, VertexProcessor processor) {
        super(metaClient, spaceName, pool, partLeaders);

        this.request = request;
        this.processor = processor;
    }


    /**
     * get the next vertex set
     *
     * @return ScanVertexResult
     */
    public ScanVertexResult next() throws IllegalAccessException, InterruptedException {
        if (!hasNext()) {
            throw new IllegalAccessException("iterator has no more data");
        }

        int partSize = partLeaders.size();
        final List<ScanVertexResult> results =
                Collections.synchronizedList(new ArrayList<>(partSize));
        final CountDownLatch countDownLatch = new CountDownLatch(partSize);

        Set<Integer> parts = partLeaders.keySet();
        for (int part : parts) {
            threadPool.submit(() -> {
                int retry = 0;
                ScanVertexRequest partRequest = new ScanVertexRequest(request);
                partRequest.setPart_id(part);
                partRequest.setCursor(partCursor.get(part));
                ScanVertexResponse response = null;
                StorageConnection connection;
                while (retry++ <= metaClient.getMetaInfo().getConnectionRetry()) {
                    try {
                        connection = pool.getStorageClient(
                                partLeaders.get(part)).getConnection();
                    } catch (Exception e) {
                        LOGGER.error(String.format("get connection of part %d error, ", part), e);
                        continue;
                    }
                    retry = Integer.MAX_VALUE;
                    int executeRetry = 0;
                    while (executeRetry++ <= metaClient.getMetaInfo().getExecutionRetry()) {
                        try {
                            response = connection.scanVertex(partRequest);
                        } catch (TException e) {
                            LOGGER.error(String.format("Scan vertex failed for retry %d: ",
                                    executeRetry), e);
                        }
                        executeRetry = Integer.MAX_VALUE;
                        if (isSuccessful(response)) {
                            partCursor.put(part, response.getNext_cursor());
                            if (!response.isHas_next()) {
                                partLeaders.remove(part);
                            }
                            DataSet dataSet = response.getVertex_data();
                            results.add(processor.constructResult(dataSet,
                                    partRequest.getReturn_columns()));
                            countDownLatch.countDown();
                            pool.release(connection.getAddress(), connection);
                            return;
                        }
                    }

                    if (response != null && response.getResult() != null) {
                        for (PartitionResult partResult : response.getResult().getFailed_parts()) {
                            if (partResult.code == ErrorCode.E_LEADER_CHANGED) {
                                freshLeader(spaceName, part, partResult.getLeader());
                            } else {
                                LOGGER.error(String.format("part scan failed while retry %d, "
                                        + "error code=%d", (retry + 1), partResult.code));
                            }
                        }
                    } else {
                        LOGGER.error("part scan failed, response result is null");
                    }
                    pool.release(connection.getAddress(), connection);
                }
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException interruptedE) {
            LOGGER.error("scan interrupted:", interruptedE);
            throw interruptedE;
        }
        hasNext = partLeaders.size() > 0;
        return processor.constructResult(results);
    }


    private boolean isSuccessful(ScanVertexResponse response) {
        if (response == null || response.result.failed_parts.size() > 0) {
            return false;
        }
        return true;
    }


    /**
     * builder to build ScanVertexResult
     */
    public static class ScanVertexResultBuilder {
        Map<Integer, HostAndPort> partLeaders;
        ScanVertexRequest request;
        VertexProcessor processor;
        MetaClient metaClient;
        String spaceName;
        StorageConnPool pool;

        public ScanVertexResultBuilder withPartLeaders(
                Map<Integer, HostAndPort> partLeaders) {
            this.partLeaders = partLeaders;
            return this;
        }

        public ScanVertexResultBuilder withRequest(ScanVertexRequest request) {
            this.request = request;
            return this;
        }

        public ScanVertexResultBuilder withProcessor(VertexProcessor processor) {
            this.processor = processor;
            return this;
        }

        public ScanVertexResultBuilder withMetaClient(MetaClient metaClient) {
            this.metaClient = metaClient;
            return this;
        }

        public ScanVertexResultBuilder withSpaceName(String spaceName) {
            this.spaceName = spaceName;
            return this;
        }

        public ScanVertexResultBuilder withPool(StorageConnPool pool) {
            this.pool = pool;
            return this;
        }

        public ScanVertexResultIterator build() {
            return new ScanVertexResultIterator(
                    metaClient,
                    spaceName,
                    pool,
                    partLeaders,
                    request,
                    processor);
        }
    }
}