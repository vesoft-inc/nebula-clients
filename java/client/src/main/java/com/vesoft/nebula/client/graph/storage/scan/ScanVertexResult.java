/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.scan;

import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.client.graph.storage.data.ScanStatus;
import com.vesoft.nebula.client.graph.storage.data.VertexRow;
import com.vesoft.nebula.client.graph.storage.data.VertexTableView;
import com.vesoft.nebula.client.graph.storage.processor.VertexProcessor;
import com.vesoft.nebula.storage.VertexProp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanVertexResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanVertexResult.class);

    private final List<DataSet> dataSets;

    private final List<VertexProp> returnCols;

    private final String tagName;

    private ScanStatus scanStatus;
    /**
     * VertexRow for table view
     */
    private List<VertexTableView> vertexTableViews;

    /**
     * schema for VertexRow's values
     */
    private List<String> propNames;

    /**
     * Vertex for structure view with prop name
     */
    private List<VertexRow> verticeRows;

    private Map<String, VertexRow> vidVertices;

    private VertexProcessor processor;

    public ScanVertexResult(List<DataSet> dataSets, List<VertexProp> returnCols, ScanStatus status,
                            String tagName, VertexProcessor processor) {
        this.dataSets = dataSets;
        this.returnCols = returnCols;
        this.scanStatus = status;
        this.tagName = tagName;
        this.processor = processor;
    }


    public String getTagName() {
        return tagName;
    }

    public List<VertexTableView> getVertexRows() {
        if (vertexTableViews == null) {
            constructVertexTableView();
        }
        return vertexTableViews;
    }

    public List<String> getPropNames() {
        if (propNames == null) {
            constrcutPropNames();
        }
        return propNames;
    }

    /**
     * get vertex with id
     *
     * @param vid Long type
     * @return Vertex
     */
    public VertexRow getVertex(String vid) {
        if (vidVertices == null) {
            constructVertexRow();
        }
        if (vidVertices.isEmpty()) {
            return null;
        }
        return vidVertices.get(vid);
    }

    /**
     * get all vertex
     *
     * @return List
     */
    public List<VertexRow> getVertices() {
        if (verticeRows == null) {
            constructVertexRow();
        }
        return verticeRows;
    }


    public Map<String, VertexRow> getVidVertices() {
        if (vidVertices == null) {
            constructVertexRow();
        }
        return vidVertices;
    }

    public boolean isAllSuccess() {
        return scanStatus == ScanStatus.ALL_SUCCESS;
    }

    private void constructVertexRow() {
        if (dataSets.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (vidVertices == null) {
                vidVertices = processor.constructVertexRow(dataSets, returnCols);
                verticeRows = new ArrayList<>(vidVertices.values());
            }
        }
    }


    private void constructVertexTableView() {
        if (dataSets.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (vertexTableViews == null) {
                vertexTableViews = processor.constructVertexTableView(dataSets);
            }
        }
    }

    private void constrcutPropNames() {
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


