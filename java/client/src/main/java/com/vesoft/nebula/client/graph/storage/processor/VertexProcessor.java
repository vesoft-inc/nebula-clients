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
import com.vesoft.nebula.client.graph.storage.data.VertexRow;
import com.vesoft.nebula.client.graph.storage.data.VertexTableView;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.TagItem;
import com.vesoft.nebula.storage.VertexProp;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

    public Map<String, VertexRow> constructVertexRow(List<DataSet> dataSets,
                                                     List<VertexProp> returnCols) {
        Map<String, VertexRow> vidVertices = Maps.newHashMap();
        // todo List<byte[]> columnNames = dataSet.getColumn_names();
        Map<Long, List<String>> colNames = getColNames(returnCols);
        for (DataSet dataSet : dataSets) {
            List<Row> rows = dataSet.getRows();
            for (Row row : rows) {
                List<Value> values = row.getValues();
                if (values.size() < 2) {
                    LOGGER.error("values size error for row: " + row.toString());
                } else {
                    Value vid = values.get(0);
                    long tagId = values.get(1).getIVal();
                    List<String> names = colNames.get(tagId);
                    Map<String, Object> props = Maps.newHashMap();
                    for (int i = 0; i < (values.size() - 2); i++) {
                        props.put(names.get(i), getField(values.get(i + 2).getFieldValue()));
                    }
                    VertexRow vertexRow = new VertexRow(vid, props);
                    try {
                        vidVertices.put(new String(vid.getSVal(), metaInfo.getDecodeType()),
                                vertexRow);
                    } catch (UnsupportedEncodingException e) {
                        LOGGER.error("encode error with " + metaInfo.getDecodeType(), e);
                    }
                }
            }
        }
        return vidVertices;
    }

    public List<VertexTableView> constructVertexTableView(List<DataSet> dataSets) {
        List<VertexTableView> vertexRows = new ArrayList<>();
        for (DataSet dataSet : dataSets) {
            List<Row> rows = dataSet.getRows();
            for (Row row : rows) {
                List<Value> values = row.getValues();
                List<Object> props = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    props.add(values.get(i).getFieldValue());
                }
                vertexRows.add(new VertexTableView(props));
            }
        }
        return vertexRows;
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
            try {
                return new String((byte[]) obj, metaInfo.getDecodeType());
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("encode error with " + metaInfo.getDecodeType(), e);
                return null;
            }
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
