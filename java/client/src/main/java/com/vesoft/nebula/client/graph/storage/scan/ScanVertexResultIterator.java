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
    private final int partSize;
    private final boolean partSuccess;
    private boolean stop = false;


    private ScanVertexResultIterator(MetaClient metaClient,
                                     String spaceName,
                                     StorageConnPool pool,
                                     Set<PartScanInfo> partScanInfoList,
                                     List<HostAndPort> addresses,
                                     ScanVertexRequest request,
                                     VertexProcessor processor,
                                     boolean partSuccess) {
        super(metaClient, spaceName, pool, new PartScanQueue(partScanInfoList), addresses);
        this.request = request;
        this.processor = processor;
        this.partSize = partScanInfoList.size();
        this.partSuccess = partSuccess;
    }


    /**
     * get the next vertex set
     *
     * @return ScanVertexResult
     */
    public ScanVertexResult next() throws Exception {
        if (!hasNext()) {
            throw new IllegalAccessException("iterator has no more data");
        }
        hasNext = false;
        final List<ScanVertexResult> results =
                Collections.synchronizedList(new ArrayList<>(partSize));
        final CountDownLatch countDownLatch = new CountDownLatch(addresses.size());

        for (HostAndPort addr : addresses) {
            threadPool.submit(() -> {
                StorageConnection connection;
                try {
                    connection = pool.getStorageClient(addr).getConnection();
                } catch (Exception e) {
                    LOGGER.error("get storage client error, ", e);
                    stop = !partSuccess;
                    return;
                }
                ScanVertexRequest partRequest = new ScanVertexRequest(request);
                ScanVertexResponse response = null;
                PartScanInfo partInfo = partScanQueue.getPart(addr);
                while (partInfo != null) {
                    partRequest.setPart_id(partInfo.getPart());
                    partRequest.setCursor(partInfo.getCursor());
                    int retry = 1;
                    while (retry-- > 0) {
                        try {
                            response = connection.scanVertex(partRequest);
                        } catch (TException e) {
                            LOGGER.error(String.format("Scan vertex failed for %s",
                                    e.getMessage()), e);
                            if (!partSuccess) {
                                stop = true;
                                return;
                            }
                        }

                        if (isSuccessful(response)) {
                            if (!response.has_next) {
                                partScanQueue.drop(partInfo);
                            } else {
                                partInfo.setCursor(response.getNext_cursor());
                            }
                            DataSet dataSet = response.getVertex_data();
                            results.add(processor.constructResult(dataSet,
                                    partRequest.getReturn_columns()));
                        }

                        if (response != null && response.getResult() != null) {
                            for (PartitionResult partResult :
                                    response.getResult().getFailed_parts()) {
                                if (partResult.code == ErrorCode.E_LEADER_CHANGED) {
                                    freshLeader(spaceName, partInfo.getPart(),
                                            partResult.getLeader());
                                    partInfo.setLeader(getLeader(partResult.getLeader()));
                                    partScanQueue.putPart(partInfo);
                                } else {
                                    LOGGER.error(String.format("part scan failed, error code=%d",
                                            partResult.code));
                                    if (partSuccess) {
                                        stop = true;
                                        return;
                                    }
                                }
                            }
                        } else {
                            LOGGER.error("part scan failed, response result is null");
                        }
                        pool.release(addr, connection);
                    }
                    partInfo = partScanQueue.getPart(addr);
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
        hasNext = partScanQueue.size() > 0;
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
        Set<PartScanInfo> partScanInfoList;
        List<HostAndPort> addresses;
        ScanVertexRequest request;
        VertexProcessor processor;
        MetaClient metaClient;
        String spaceName;
        StorageConnPool pool;
        boolean partSuccess;

        public ScanVertexResultBuilder withPartLeaders(
                Set<PartScanInfo> partScanInfoList) {
            this.partScanInfoList = partScanInfoList;
            return this;
        }

        public ScanVertexResultBuilder withAddresses(List<HostAndPort> addresses) {
            this.addresses = addresses;
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

        public ScanVertexResultBuilder withPartSuccess(boolean partSuccess) {
            this.partSuccess = partSuccess;
            return this;
        }

        public ScanVertexResultIterator build() {
            return new ScanVertexResultIterator(
                    metaClient,
                    spaceName,
                    pool,
                    partScanInfoList,
                    addresses,
                    request,
                    processor,
                    partSuccess);
        }
    }
}