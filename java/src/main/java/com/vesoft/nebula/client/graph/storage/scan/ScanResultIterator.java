/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.google.common.net.HostAndPort;
import com.vesoft.nebula.HostAddr;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import com.vesoft.nebula.client.graph.storage.StorageConnPool;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ScanResultIterator {
    protected final Map<Integer, byte[]> partCursor;
    protected boolean hasNext = true;

    protected final ConcurrentHashMap<Integer, HostAndPort> partLeaders;
    protected final MetaClient metaClient;
    protected final String spaceName;
    protected final StorageConnPool pool;
    protected final ExecutorService threadPool;

    protected ScanResultIterator(MetaClient metaClient, String spaceName, StorageConnPool pool,
                                     Map<Integer, HostAndPort> partLeaders) {
        this.metaClient = metaClient;
        this.spaceName = spaceName;
        this.pool = pool;
        this.partLeaders = new ConcurrentHashMap<>(partLeaders);
        this.partCursor = new HashMap<>(partLeaders.size());
        this.threadPool = Executors.newFixedThreadPool(partLeaders.keySet().size());
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
        HostAndPort newLeader = HostAndPort.fromParts(leader.getHost(), leader.getPort());
        metaClient.freshLeader(spaceName, part, newLeader);
    }

}
