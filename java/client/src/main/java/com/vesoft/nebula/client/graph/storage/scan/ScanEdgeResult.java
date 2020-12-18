/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.client.graph.storage.data.EdgeRow;
import com.vesoft.nebula.client.graph.storage.data.EdgeTableView;
import com.vesoft.nebula.client.graph.storage.data.ScanStatus;
import com.vesoft.nebula.client.graph.storage.processor.EdgeProcessor;
import com.vesoft.nebula.storage.EdgeProp;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanEdgeResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanEdgeResult.class);

    private final List<DataSet> dataSets;

    private final List<EdgeProp> returnCols;

    private final String edgeName;

    /**
     * scan result
     */
    private final ScanStatus scanStatus;
    /**
     * VertexRow for table view
     */
    private List<EdgeTableView> edgeTableViews;

    /**
     * schema for VertexRow's values
     */
    private List<String> propNames;

    /**
     * Vertex for structure view with prop name
     */
    private List<EdgeRow> edgeRows;

    private final EdgeProcessor processor;

    public ScanEdgeResult(List<DataSet> dataSets, List<EdgeProp> returnCols, ScanStatus status,
                          String edgeName, EdgeProcessor processor) {
        this.dataSets = dataSets;
        this.returnCols = returnCols;
        this.scanStatus = status;
        this.edgeName = edgeName;
        this.processor = processor;
    }

    public String getEdgeName() {
        return edgeName;
    }

    public List<EdgeTableView> getEdgeTableViews() {
        if (edgeTableViews == null) {
            constructEdgeRow();
        }
        return edgeTableViews;
    }

    public List<String> getPropNames() {
        if (propNames == null) {
            constructPropNames();
        }
        return propNames;
    }

    public List<EdgeRow> getEdges() {
        if (edgeRows == null) {
            constructEdgeRow();
        }
        return edgeRows;
    }

    public boolean isAllSuccess() {
        return scanStatus == ScanStatus.ALL_SUCCESS;
    }

    private void constructEdgeTableView() {
        if (dataSets.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (edgeTableViews == null) {
                edgeTableViews = processor.constructEdgeTableView(dataSets);
            }
        }
    }

    private void constructEdgeRow() {
        if (dataSets.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (edgeRows == null) {
                edgeRows = processor.constructEdgeRow(dataSets, returnCols);
            }
        }
    }

    private void constructPropNames() {
        if (dataSets.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (propNames == null) {
                propNames = new ArrayList<>();
                List<byte[]> colNames = dataSets.get(0).getColumn_names();
                for (byte[] colName : colNames) {
                    propNames.add(new String(colName));
                }
            }
        }
    }
}
