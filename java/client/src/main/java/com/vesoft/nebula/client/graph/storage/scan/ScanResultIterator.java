/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.google.common.net.HostAndPort;
import com.vesoft.nebula.HostAddr;
import com.vesoft.nebula.client.graph.exception.ExecuteFailedException;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import com.vesoft.nebula.client.graph.storage.StorageConnPool;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanResultIterator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanResultIterator.class);

    protected boolean hasNext = true;

    protected final Map<Integer, byte[]> partCursor;
    protected final ExecutorService threadPool;

    protected final MetaClient metaClient;
    protected final StorageConnPool pool;
    protected final PartScanQueue partScanQueue;
    protected final Set<HostAndPort> addresses;
    protected final String spaceName;
    protected final String labelName;
    protected final boolean partSuccess;

    protected ScanResultIterator(MetaClient metaClient, StorageConnPool pool,
                                 PartScanQueue partScanQueue, Set<HostAndPort> addresses,
                                 String spaceName, String labelName, boolean partSuccess) {
        this.metaClient = metaClient;
        this.pool = pool;
        this.partScanQueue = partScanQueue;
        this.addresses = addresses;
        this.spaceName = spaceName;
        this.labelName = labelName;
        this.partSuccess = partSuccess;

        this.threadPool = Executors.newFixedThreadPool(addresses.size());
        this.partCursor = new HashMap<>(partScanQueue.size());
    }


    /**
     * if iter has more vertex data
     *
     * @return boolean
     */
    public boolean hasNext() {
        return hasNext;
    }


    /**
     * fresh leader for part
     *
     * @param spaceName nebula graph space
     * @param part      part
     * @param leader    part new leader
     */
    protected void freshLeader(String spaceName, int part, HostAddr leader) {
        HostAndPort newLeader = getLeader(leader);
        metaClient.freshLeader(spaceName, part, newLeader);
    }

    protected HostAndPort getLeader(HostAddr leader) {
        return HostAndPort.fromParts(leader.getHost(), leader.getPort());
    }

    protected void handleNullResult(PartScanInfo partInfo, List<Exception> exceptions) {
        LOGGER.error("part scan failed, response result is null");
        partScanQueue.drop(partInfo);
        exceptions.add(new Exception("null scan response"));
    }

    protected void throwExceptions(List<Exception> exceptions) throws ExecuteFailedException {
        StringBuilder errorMsg = new StringBuilder();
        for (Exception e : exceptions) {
            errorMsg.append(e.getMessage());
        }
        throw new ExecuteFailedException("no parts succeed, error message: " + errorMsg.toString());
    }
}
