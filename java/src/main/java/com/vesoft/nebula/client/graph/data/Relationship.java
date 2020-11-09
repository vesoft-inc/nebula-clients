/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.data;

import com.vesoft.nebula.Edge;
import com.vesoft.nebula.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Relationship {
    private String srcId = null;
    private String dstId = null;
    private String edgeName = null;
    private long ranking = 0;
    private int type = 0;
    private List<String> propNames = new ArrayList<>();
    private List<Value> propValues = new ArrayList<>();

    public Relationship(Edge edge) {
        if (edge == null) {
            return;
        }
        this.srcId = new String(edge.src);
        this.dstId = new String(edge.dst);
        this.ranking = edge.ranking;
        this.edgeName = new String(edge.name);
        this.type = edge.type;
        for (byte[] name : edge.props.keySet()) {
            this.propNames.add(new String(name));
            this.propValues.add(edge.props.get(name));
        }
    }

    public List<String> getPropNames() {
        return propNames;
    }

    public List<Value> getPropValues() {
        return propValues;
    }

    public String getSrcId() {
        return srcId;
    }

    public String getDstId() {
        return dstId;
    }

    public String getEdgeName() {
        return edgeName;
    }

    public long getRanking() {
        return ranking;
    }

    public Value getPropValue(String propName) {
        int index = propNames.indexOf(propName);
        if (index < 0) {
            throw new IllegalArgumentException(propName + " is not found");
        }
        return propValues.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Relationship that = (Relationship) o;
        return ranking == that.ranking
                && type == that.type
                && Objects.equals(srcId, that.srcId)
                && Objects.equals(dstId, that.dstId)
                && Objects.equals(edgeName, that.edgeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcId, dstId, edgeName, ranking, propNames, propValues);
    }

    @Override
    public String toString() {
        return "Relationship{"
                + "srcId='" + srcId
                + ", dstId='" + dstId
                + ", edgeName='" + edgeName
                + ", ranking=" + ranking
                + ", propNames=" + propNames
                + ", propValues=" + propValues
                + '}';
    }
}
