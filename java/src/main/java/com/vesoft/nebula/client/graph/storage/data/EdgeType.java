/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import java.util.Map;

public class EdgeType extends Label {
    private final long rank;

    public EdgeType(String name, long rank, Map<String, Object> prop) {
        super(name, prop);
        this.rank = rank;
    }

    public long getRank() {
        return rank;
    }

    @Override
    public String toString() {
        return "EdgeType{"
                + "rank=" + rank + ","
                + super.toString();
    }
}
