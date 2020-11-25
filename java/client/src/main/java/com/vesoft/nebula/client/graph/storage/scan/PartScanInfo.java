/*
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.google.common.net.HostAndPort;

public class PartScanInfo {

    private int part;
    private HostAndPort leader;
    private byte[] cursor = null;

    public PartScanInfo(int part, HostAndPort leader) {
        this.part = part;
        this.leader = leader;
    }

    public int getPart() {
        return part;
    }

    public void setPart(int part) {
        this.part = part;
    }

    public HostAndPort getLeader() {
        return leader;
    }

    public void setLeader(HostAndPort leader) {
        this.leader = leader;
    }

    public byte[] getCursor() {
        return cursor;
    }

    public void setCursor(byte[] cursor) {
        this.cursor = cursor;
    }

    @Override
    public String toString() {
        return "PartScanInfo{"
                + "part=" + part
                + ", leader=" + leader
                + ", cursor=" + new String(cursor)
                + '}';
    }
}
