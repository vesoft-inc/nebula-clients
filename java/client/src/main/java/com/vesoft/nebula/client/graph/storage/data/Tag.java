/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import com.vesoft.nebula.Value;
import java.util.Map;

public class Tag extends Label {

    public Tag(String name, Map<String, Value> props) {
        super(name, props);
    }

    @Override
    public String toString() {
        return "Tag{" + super.toString();
    }
}
