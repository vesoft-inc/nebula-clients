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
import com.vesoft.nebula.client.graph.storage.data.Edge;
import com.vesoft.nebula.client.graph.storage.data.EdgeType;
import com.vesoft.nebula.client.graph.storage.scan.ScanEdgeResult;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.storage.EdgeProp;
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

    public ScanEdgeResult constructResult(DataSet dataSet, List<EdgeProp> returnCols) {
        List<Edge> edges = new ArrayList<>();
        Map<String, List<Edge>> labelEdges = Maps.newHashMap();
        // TODO use columnNames returned by server: List<byte[]> columnNames = dataSet
        //  .getColumn_names();
        List<Row> rows = dataSet.getRows();
        Map<String, List<EdgeType>> edgeTyps = Maps.newHashMap();

        Map<Long, List<String>> colNames = getColNames(returnCols);
        for (Row row : rows) {
            List<Value> values = row.getValues();
            if (values.size() < 2) {
                LOGGER.error("values size error for row: " + row.toString());
            } else {
                String srcId = new String(values.get(0).getSVal());
                long edgeId = values.get(1).getIVal();
                long rank = values.get(2).getIVal();
                String dstId = new String(values.get(3).getSVal());
                List<String> names = colNames.get(edgeId);
                Map<String, Object> props = Maps.newHashMap();
                for (int i = 0; i < (values.size() - 4); i++) {
                    props.put(
                            names.get(i),
                            getField(values.get(i + 4).getFieldValue()));
                }
                String edgeName = getEdgeName(spaceName, edgeId);
                EdgeType edgeType = new EdgeType(edgeName, rank, props);
                Edge edge = new Edge(srcId, dstId, edgeType);
                edges.add(edge);
                if (!labelEdges.containsKey(edgeName)) {
                    labelEdges.put(edgeName, new ArrayList<>());
                }
                labelEdges.get(edgeName).add(edge);
            }
        }

        return new ScanEdgeResult(edges, labelEdges);
    }


    public ScanEdgeResult constructResult(List<ScanEdgeResult> results) {
        List<Edge> edges = new ArrayList<>();
        Map<String, List<Edge>> labelEdges = Maps.newHashMap();
        for (ScanEdgeResult result : results) {
            edges.addAll(result.getAllEdges());
            Map<String, List<Edge>> labelEs = result.getLabelEdges();
            for (Map.Entry<String, List<Edge>> entry : labelEs.entrySet()) {
                if (!labelEdges.containsKey(entry.getKey())) {
                    labelEdges.put(entry.getKey(), new ArrayList<>());
                }
                labelEdges.get(entry.getKey()).addAll(entry.getValue());
            }
        }
        return new ScanEdgeResult(edges, labelEdges);

    }

    /**
     * get edge name according to edge id
     *
     * @param spaceName nebula graph space
     * @param edgeId    edge id
     * @return String
     */
    private String getEdgeName(String spaceName, long edgeId) {
        return metaInfo.getEdgeIdMap().get(spaceName).get(edgeId);
    }

    private Object getField(Object obj) {
        if (obj.getClass().getTypeName().equals("byte[]")) {
            return new String((byte[]) obj);
        }
        return obj;
    }

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
