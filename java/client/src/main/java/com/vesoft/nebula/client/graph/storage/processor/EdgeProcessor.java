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
import com.vesoft.nebula.client.graph.storage.data.EdgeRow;
import com.vesoft.nebula.client.graph.storage.data.EdgeTableView;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.storage.EdgeProp;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdgeProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeProcessor.class);

    private final MetaInfo metaInfo;
    private final String spaceName;

    public EdgeProcessor(String spaceName, MetaInfo metaInfo) {
        this.metaInfo = metaInfo;
        this.spaceName = spaceName;
    }

    public List<EdgeRow> constructEdgeRow(List<DataSet> dataSets, List<EdgeProp> returnCols) {
        List<EdgeRow> edgeRows = new ArrayList<>();
        Map<Long, List<String>> colNames = getColNames(returnCols);
        for (DataSet dataSet : dataSets) {
            List<Row> rows = dataSet.getRows();
            for (Row row : rows) {
                List<Value> values = row.getValues();
                if (values.size() < 4) {
                    LOGGER.error("values size error for row: " + row.toString());
                } else {
                    Value srcId = values.get(0);
                    long edgeId = values.get(1).getIVal();
                    Value rank = values.get(2);
                    Value dstId = values.get(3);
                    List<String> names = colNames.get(edgeId);
                    Map<String, Object> props = Maps.newHashMap();
                    for (int i = 0; i < (values.size() - 4); i++) {
                        props.put(names.get(i), getField(values.get(i + 4).getFieldValue()));
                    }
                    EdgeRow edgeRow = new EdgeRow(srcId, dstId, rank, props);
                    edgeRows.add(edgeRow);
                }
            }
        }
        return edgeRows;
    }

    public List<EdgeTableView> constructEdgeTableView(List<DataSet> dataSets) {
        List<EdgeTableView> edgeRows = new ArrayList<>();
        for (DataSet dataSet : dataSets) {
            List<Row> rows = dataSet.getRows();
            for (Row row : rows) {
                List<Value> values = row.getValues();
                List<Object> props = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    props.add(values.get(i).getFieldValue());
                }
                edgeRows.add(new EdgeTableView(props));
            }
        }
        return edgeRows;
    }


    /**
     * get edgeRow name according to edgeRow id
     *
     * @param spaceName nebula graph space
     * @param edgeId    edgeRow id
     * @return String
     */
    private String getEdgeName(String spaceName, long edgeId) {
        return metaInfo.getEdgeIdMap().get(spaceName).get(edgeId);
    }

    /**
     * get decoded field
     */
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

    /**
     * get col names according to scan parameters
     *
     * @param returnCols scan parameters
     * @return
     */
    private Map<Long, List<String>> getColNames(List<EdgeProp> returnCols) {
        Map<Long, List<String>> colNames = new HashMap<>();
        if (returnCols.isEmpty()) {
            Set<Long> edges = metaInfo.getSpaceEdgeItems().get(spaceName).keySet();
            for (long edgeId : edges) {
                EdgeItem edgeItem = metaInfo.getSpaceEdgeItems().get(spaceName).get(edgeId);
                List<String> names = new ArrayList<>();
                for (ColumnDef colDef : edgeItem.getSchema().getColumns()) {
                    names.add(new String(colDef.name));
                }
                colNames.put(edgeId, names);
            }
            return colNames;
        }

        for (EdgeProp edgeProp : returnCols) {
            List<String> names = new ArrayList<>();
            if (edgeProp.getProps().isEmpty()) {
                EdgeItem edgeItem =
                        metaInfo.getSpaceEdgeItems().get(spaceName).get((long) edgeProp.getType());
                for (ColumnDef colDef : edgeItem.getSchema().getColumns()) {
                    names.add(new String(colDef.name));
                }
            } else {
                for (byte[] prop : edgeProp.getProps()) {
                    names.add(new String(prop));
                }
            }
            colNames.put((long) edgeProp.getType(), names);
        }
        return colNames;
    }
}
