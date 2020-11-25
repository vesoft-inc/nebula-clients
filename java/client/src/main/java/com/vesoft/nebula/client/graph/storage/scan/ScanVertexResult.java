/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.google.common.collect.Maps;
import com.vesoft.nebula.client.graph.storage.data.Vertex;
import com.vesoft.nebula.client.graph.storage.data.VertexDataSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanVertexResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanVertexResult.class);

    private final List<VertexDataSet> vertexDataSets;
    private List<Vertex> vertices = new ArrayList<>();
    private Map<String, Vertex> vidVertices = Maps.newHashMap();
    private Map<String, List<Vertex>> labelVertices = Maps.newHashMap();

    public ScanVertexResult(List<VertexDataSet> vertexDataSets) {
        this.vertexDataSets = vertexDataSets;
    }

    /**
     * get all vertex of tag
     *
     * @param tag vertex type
     * @return List
     */
    public List<Vertex> getVertices(String tag) {
        if (StringUtils.isBlank(tag) || !labelVertices.containsKey(tag)) {
            throw new IllegalArgumentException(String.format("label %s doesn't exist.", tag));
        }
        return labelVertices.get(tag);
    }

    /**
     * get vertex with id
     *
     * @param vid Long type
     * @return Vertex
     */
    public Vertex getVertex(String vid) {
        if (!vidVertices.containsKey(vid)) {
            throw new IllegalArgumentException(String.format("vertex %d doesn't exist.", vid));
        }
        return vidVertices.get(vid);
    }

    /**
     * get all vertex
     *
     * @return List
     */
    public List<Vertex> getAllVertices() {
        return vertices;
    }


    public Map<String, Vertex> getVidVertices() {
        return vidVertices;
    }

    public Map<String, List<Vertex>> getLabelVertices() {
        return labelVertices;
    }

    @Override
    public String toString() {
        return "ScanVertexResult{"
                + "vertices=" + vertices
                + ", vidVertices=" + vidVertices
                + ", labelVertices=" + labelVertices
                + '}';
    }
}


