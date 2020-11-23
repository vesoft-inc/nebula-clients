/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import com.vesoft.nebula.Value;
import java.util.Map;

public class Label {
    private String name;
    private Map<String, Value> props;

    public Label(String name, Map<String, Value> props) {
        this.name = name;
        this.props = props;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Value> getProps() {
        return props;
    }

    public void setProps(Map<String, Value> props) {
        this.props = props;
    }

    @Override
    public String toString() {
        return ""
                + "name='" + new String(name) + '\''
                + ", props=" + props
                + '}';
    }
}
