/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import com.vesoft.nebula.Value;
import java.util.Map;

public class EdgeType extends Label {

    public EdgeType(String name, Map<String, Value> prop) {
        super(name, prop);
    }


    @Override
    public String toString() {
        return "EdgeType{"
                + super.toString();
    }
}
