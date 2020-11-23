/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.processor;

import com.google.common.collect.Maps;
import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.meta.MetaInfo;
import com.vesoft.nebula.client.graph.storage.data.Tag;
import com.vesoft.nebula.client.graph.storage.data.Vertex;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResult;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.TagItem;
import com.vesoft.nebula.storage.VertexProp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertexProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexProcessor.class);

    private final MetaInfo metaInfo;
    private final String spaceName;

    public VertexProcessor(String spaceName, MetaInfo metaInfo) {
        this.metaInfo = metaInfo;
        this.spaceName = spaceName;
    }

    public ScanVertexResult constructResult(DataSet dataSet, List<VertexProp> returnCols) {
        List<Vertex> vertices = new ArrayList<>();
        Map<String, Vertex> vidVertices = Maps.newHashMap();
        Map<String, List<Vertex>> labelVertices = Maps.newHashMap();
        // todo List<byte[]> columnNames = dataSet.getColumn_names();
        List<Row> rows = dataSet.getRows();
        Map<String, List<Tag>> vertexTags = Maps.newHashMap();

        Map<Long, List<String>> colNames = getColNames(returnCols);
        for (Row row : rows) {
            List<Value> values = row.getValues();
            if (values.size() < 2) {
                LOGGER.error("values size error for row: " + row.toString());
            } else {
                String vid = new String(values.get(0).getSVal());
                long tagId = values.get(1).getIVal();
                List<String> names = colNames.get(tagId);
                Map<String, Value> props = Maps.newHashMap();
                for (int i = 0; i < (values.size() - 2); i++) {
                    props.put(names.get(i), values.get(i + 2));
                }
                Tag tag = new Tag(getTagName(spaceName, tagId), props);
                if (vertexTags.containsKey(vid)) {
                    vertexTags.get(vid).add(tag);
                } else {
                    vertexTags.put(vid, Collections.singletonList(tag));
                }
            }
        }

        // construct vertices/vidVertices/labelVertices
        for (Map.Entry<String, List<Tag>> vertexTag : vertexTags.entrySet()) {
            Vertex vertex = new Vertex(vertexTag.getKey(), vertexTag.getValue());
            vertices.add(vertex);
            vidVertices.put(vertexTag.getKey(), vertex);
            for (Tag tag : vertexTag.getValue()) {
                if (!labelVertices.containsKey(tag.getName())) {
                    labelVertices.put(tag.getName(), new ArrayList<>());
                }
                labelVertices.get(tag.getName()).add(vertex);
            }
        }
        return new ScanVertexResult(vertices, vidVertices, labelVertices);
    }

    public ScanVertexResult constructResult(List<ScanVertexResult> results) {
        List<Vertex> vertices = new ArrayList<>();
        Map<String, Vertex> vidVertices = Maps.newHashMap();
        Map<String, List<Vertex>> labelVertices = Maps.newHashMap();
        for (ScanVertexResult result : results) {
            vertices.addAll(result.getAllVertices());
            vidVertices.putAll(result.getVidVertices());
            Map<String, List<Vertex>> labelVs = result.getLabelVertices();
            for (Map.Entry<String, List<Vertex>> entry : labelVs.entrySet()) {
                if (!labelVertices.containsKey(entry.getKey())) {
                    labelVertices.put(entry.getKey(), new ArrayList<>());
                }
                labelVertices.get(entry.getKey()).addAll(entry.getValue());
            }
        }
        return new ScanVertexResult(vertices, vidVertices, labelVertices);
    }


    /**
     * get tag name according to tag id
     *
     * @param spaceName nebula graph space
     * @param tagId     tag id
     * @return String
     */
    private String getTagName(String spaceName, long tagId) {
        return metaInfo.getTagIdMap().get(spaceName).get(tagId);
    }

    private Object getField(Object obj) {
        if (obj.getClass().getTypeName().equals("byte[]")) {
            return new String((byte[]) obj);
        }
        return obj;
    }

    private Map<Long, List<String>> getColNames(List<VertexProp> returnCols) {
        Map<Long, List<String>> colNames = new HashMap<>();
        if (returnCols.isEmpty()) {
            Set<Long> tags = metaInfo.getSpaceTagItems().get(spaceName).keySet();
            for (long tagId : tags) {
                TagItem tagItem = metaInfo.getSpaceTagItems().get(spaceName).get(tagId);
                List<String> names = new ArrayList<>();
                for (ColumnDef colDef : tagItem.getSchema().getColumns()) {
                    names.add(new String(colDef.getName()));
                }
                colNames.put(tagId, names);
            }
            return colNames;
        }

        for (VertexProp vertexProp : returnCols) {
            List<String> names = new ArrayList<>();
            if (vertexProp.getProps().isEmpty()) {
                TagItem tagItem =
                        metaInfo.getSpaceTagItems().get(spaceName).get((long) vertexProp.getTag());
                for (ColumnDef colDef : tagItem.getSchema().getColumns()) {
                    names.add(new String(colDef.name));
                }
            } else {
                for (byte[] prop : vertexProp.getProps()) {
                    names.add(new String(prop));
                }
            }
            colNames.put((long) vertexProp.getTag(), names);
        }
        return colNames;
    }
}
