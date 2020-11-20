/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.vesoft.nebula.client.graph.storage.data.Edge;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanEdgeResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanEdgeResult.class);

    private final List<Edge> edges;
    private final Map<String, List<Edge>> labelEdges;

    public ScanEdgeResult(List<Edge> edges, Map<String, List<Edge>> labelEdges) {
        this.edges = edges;
        this.labelEdges = labelEdges;
    }

    /**
     * get all edge of tag
     *
     * @param edge edge type
     * @return List
     */
    public List<Edge> getEdges(String edge) {
        if (StringUtils.isBlank(edge) || !labelEdges.containsKey(edge)) {
            throw new IllegalArgumentException(String.format("label %s doesn't exist.", edge));
        }
        return labelEdges.get(edge);
    }


    /**
     * get all edge
     *
     * @return List
     */
    public List<Edge> getAllEdges() {
        return edges;
    }

    public Map<String, List<Edge>> getLabelEdges() {
        return labelEdges;
    }

    @Override
    public String toString() {
        return "ScanEdgeResult{"
                + "edges=" + edges
                + ", labelEdges=" + labelEdges
                + '}';
    }
}
