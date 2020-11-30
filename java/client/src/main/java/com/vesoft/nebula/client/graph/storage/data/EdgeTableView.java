/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class EdgeTableView {
    private final List<Object> values;
    private String decodeType = "utf-8";

    public EdgeTableView(List<Object> values) {
        this.values = values;
    }

    public EdgeTableView(List<Object> values, String decodeType) {
        this.values = values;
        this.decodeType = decodeType;
    }

    // // todo if server changes the scan result, then values.size is not at least 4.
    public String getSrcId() throws UnsupportedEncodingException {
        if (values.size() < 4) {
            throw new IllegalArgumentException("no src id is returned");
        }
        return new String((byte[]) values.get(0), decodeType);
    }

    public String getDstId() throws UnsupportedEncodingException {
        if (values.size() < 4) {
            throw new IllegalArgumentException("no dst id is returned");
        }
        return new String((byte[]) values.get(3), decodeType);
    }

    public long getEdgeId() {
        if (values.size() < 4) {
            throw new IllegalArgumentException("no edge id is returned");
        }
        return (long) values.get(1);
    }

    public long getRank() {
        if (values.size() < 4) {
            throw new IllegalArgumentException("no rank is returned");
        }
        return (long) values.get(2);
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        try {
            return "EdgeTableView{"
                    + "srcId=" + getSrcId()
                    + ", dstId=" + getDstId()
                    + ", rank=" + getRank()
                    + ", values=" + getValues()
                    + '}';
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
