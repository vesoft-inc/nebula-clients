/*
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.google.common.net.HostAndPort;
import java.util.Set;

public class PartScanQueue {

    private final Set<PartScanInfo> partScanInfoSet;

    public PartScanQueue(Set<PartScanInfo> partScanInfoSet) {
        this.partScanInfoSet = partScanInfoSet;
    }

    /**
     * get part according to leader
     *
     * @return null if no match part
     */
    public synchronized PartScanInfo getPart(HostAndPort leader) {
        for (PartScanInfo partScanInfo : partScanInfoSet) {
            if (partScanInfo.getLeader().equals(leader)) {
                return partScanInfo;
            }
        }
        return null;
    }

    /**
     * put part with new leader
     */
    public synchronized void putPart(PartScanInfo partScanInfo) {
        partScanInfoSet.add(partScanInfo);
    }

    /**
     * delete part from set
     */
    public synchronized void drop(PartScanInfo partScanInfo) {
        partScanInfoSet.remove(partScanInfo);
    }

    public int size() {
        return partScanInfoSet.size();
    }

}
