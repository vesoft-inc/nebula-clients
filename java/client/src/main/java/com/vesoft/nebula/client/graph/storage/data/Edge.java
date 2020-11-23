/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

public class Edge {
    private final byte[] srcId;
    private final byte[] dstId;
    private final long rank;
    private final EdgeType edgeType;

    public Edge(byte[] srcId, byte[] dstId, long rank, EdgeType edgeType) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.rank = rank;
        this.edgeType = edgeType;
    }

    public String getSrcId() {
        return new String(srcId);
    }

    public String getDstId() {
        return new String(dstId);
    }

    public long getRank() {
        return rank;
    }

    public EdgeType getEdgeType() {
        return edgeType;
    }

    @Override
    public String toString() {
        return "Edge{"
                + "srcId='" + new String(srcId) + '\''
                + ", dstId='" + new String(dstId) + '\''
                + ", rank=" + rank
                + ", edgeType=" + edgeType
                + '}';
    }
}
