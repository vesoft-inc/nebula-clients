/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.processor;

import com.google.common.collect.Maps;
import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.Date;
import com.vesoft.nebula.DateTime;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.meta.MetaInfo;
import com.vesoft.nebula.client.graph.storage.scan.ScanVertexResult;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.ColumnTypeDef;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.SchemaProp;
import com.vesoft.nebula.meta.TagItem;
import com.vesoft.nebula.storage.VertexProp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;

public class VertexProcessorTest extends TestCase {
    DataSet dataSet;
    VertexProcessor processor;
    MetaInfo metaInfo = new MetaInfo();

    private final String spaceName = "test";
    private final String vid = "1";
    private final long tagId = 1111;
    private final String tagName = "testTag";

    public void setUp() throws Exception {
        super.setUp();
        dataSet = mockDataSet();
        metaInfo = mockMetaInfo();
        processor = new VertexProcessor(spaceName, metaInfo);
    }

    public void tearDown() throws Exception {

    }

    public void testConstructResult() {
        List<VertexProp> returnCols = Arrays.asList(new VertexProp((int) tagId, new ArrayList<>()));
        ScanVertexResult result = processor.constructResult(dataSet, returnCols);
        assert result.getAllVertices().size() == 1;
        assert result.getVertices(tagName).size() == 1;
        assert result.getVertex(vid).getTags().size() == 1;
        assert result.getVertex(vid).getTags().get(0).getName().equals(tagName);
    }

    private DataSet mockDataSet() {

        List<byte[]> columns = new ArrayList<>();
        columns.add("name".getBytes());
        // columns.add("boolProp".getBytes());
        // columns.add("intProp".getBytes());
        // columns.add("longProp".getBytes());
        // columns.add("doubleProp".getBytes());
        // columns.add("dateProp".getBytes());
        // columns.add("dataTimeProp".getBytes());

        List<Value> values = new ArrayList<>();
        // vid and tagId
        values.add(Value.sVal(vid.getBytes()));
        values.add(Value.iVal(tagId));
        // prop values
        values.add(Value.sVal("Jena".getBytes()));
        //values.add(Value.bVal(true));
        //values.add(Value.nVal(1));
        //values.add(Value.iVal(2L));
        //values.add(Value.fVal(1.0));
        short year = 2020;
        byte month = 11;
        byte day = 16;
        //values.add(Value.dVal(new Date(year, month, day)));
        byte hour = 12;
        byte minute = 30;
        byte sec = 30;
        int microsec = 5;
        //values.add(Value.dtVal(new DateTime(year, month, day, hour, minute, sec, microsec)));

        Row row = new Row(values);
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        return new DataSet(columns, rows);
    }

    private MetaInfo mockMetaInfo() {
        MetaInfo metaInfo = new MetaInfo();
        Map<Long, String> map = Maps.newHashMap();
        map.put(tagId, tagName);
        metaInfo.getTagIdMap().put(spaceName, map);

        TagItem tagItem = mockTagItem();
        Map<Long, TagItem> tagItemMap = Maps.newHashMap();
        tagItemMap.put(tagId, tagItem);
        metaInfo.getSpaceTagItems().put(spaceName, tagItemMap);
        return metaInfo;
    }

    private TagItem mockTagItem() {
        Schema schema = new Schema(
                Arrays.asList(
                        new ColumnDef("name".getBytes(),
                                new ColumnTypeDef(6))), new SchemaProp());
        TagItem tagItem = new TagItem((int) tagId, tagName.getBytes(), 1L, schema);
        return tagItem;
    }
}