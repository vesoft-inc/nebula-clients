/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class VertexTableView {
    private final List<Object> values;
    private String decodeType = "utf-8";

    public VertexTableView(List<Object> values) {
        this.values = values;
    }

    public VertexTableView(List<Object> values, String decodeType) {
        this.values = values;
        this.decodeType = decodeType;
    }

    // todo if server changes the scan result, then values.size is not at least 2.
    public String getVid() throws UnsupportedEncodingException {
        if (values.size() < 2) {
            throw new IllegalArgumentException("no vertex id is returned");
        }
        return new String((byte[]) values.get(0), decodeType);
    }

    public long getTagId() {
        if (values.size() < 2) {
            throw new IllegalArgumentException("no tag id is returned");
        }
        return (long) values.get(1);
    }

    public List<Object> getValues() {
        return values;
    }


    @Override
    public String toString() {
        try {
            return "VertexTableView{"
                    + "vid=" + getVid()
                    + ", values=" + getValues()
                    + '}';
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
