package com.vesoft.nebula.client.graph.data;

import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.Edge;
import com.vesoft.nebula.Path;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Step;
import com.vesoft.nebula.Tag;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.Vertex;
import com.vesoft.nebula.graph.ErrorCode;
import com.vesoft.nebula.graph.ExecutionResponse;
import com.vesoft.nebula.graph.PlanDescription;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.Test;


public class TestData {
    public Vertex getVertex(String vid) {
        Vertex vertex = new Vertex();
        List<Tag> tags = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<byte[], Value> props = new HashMap<>();
            for (int j = 0; j < 5; j++) {
                Value value = new Value();
                value.setIVal(j);
                props.put(String.format("prop%d", j).getBytes(), value);
            }
            Tag tag = new Tag(String.format("tag%d", i).getBytes(), props);
            tags.add(tag);
        }
        return new Vertex(vid.getBytes(), tags);
    }

    public Edge getEdge(String srcId, String dstId) {
        Edge edge = new Edge();
        edge.setSrc(srcId.getBytes());
        edge.setDst(dstId.getBytes());
        Map<byte[], Value> props = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Value value = new Value();
            value.setIVal(i);
            props.put(String.format("prop%d", i).getBytes(), value);
        }
        return new Edge(srcId.getBytes(),
                dstId.getBytes(),
                1,
                "classmate".getBytes(),
                100,
                props);
    }

    public Path getPath(String startId, int stepsNum) {
        List<Step> steps = new ArrayList<>();
        for (int i = 0; i < stepsNum; i++) {
            Map<byte[], Value> props = new HashMap<>();
            for (int j = 0; j < 5; j++) {
                Value value = new Value();
                value.setIVal(j);
                props.put(String.format("prop%d", j).getBytes(), value);
            }
            int type = 1;
            if (i % 2 != 0) {
                type = -1;
            }
            Vertex dstId = getVertex(String.format("vertex%d", i));
            steps.add(new Step(getVertex(String.format("vertex%d", i)),
                    type,
                    String.format("classmate").getBytes(), 100, props));
        }
        return new Path(getVertex(startId), steps);
    }

    public DataSet getDateset() {
        List<byte[]> columnNames = Arrays.asList(
                "col0_int".getBytes(),
                "col1_string".getBytes(),
                "col2_vertex".getBytes(),
                "col3_edge".getBytes(),
                "col4_path".getBytes());
        Row row = new Row(Arrays.asList(new Value(Value.IVAL, new Long(1)),
                new Value(Value.SVAL, "value1".getBytes()),
                new Value(Value.VVAL, getVertex("Tom")),
                new Value(Value.EVAL, getEdge("Tom", "Lily")),
                new Value(Value.PVAL, getPath("Tom", 3))));
        return new DataSet(columnNames, Arrays.asList(row));
    }

    @Test
    public void testNode() {
        try {
            Node node = new Node(getVertex(new String("Tom")));
            assert Objects.equals(node.getId(), "Tom");
            assert node.hasTag("tag1");
            List<String> names = Arrays.asList("prop0", "prop1", "prop2", "prop3", "prop4");
            assert Objects.equals(
                    node.propNames("tag0").stream().sorted().collect(Collectors.toList()),
                    names.stream().sorted().collect(Collectors.toList()));
            assert Objects.equals(node.getValue("tag1", "prop1"),
                                  new Value(Value.IVAL,1L));
            List<Value> propValues = Arrays.asList(new Value(Value.IVAL, 0L),
                                                   new Value(Value.IVAL, 1L),
                                                   new Value(Value.IVAL, 2L),
                                                   new Value(Value.IVAL, 3L),
                                                   new Value(Value.IVAL, 4L));

            // TODO: Check the List<Value>
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testRelationShip() {
        try {
            Edge edge = getEdge(new String("Tom"), new String("Lily"));
            Relationship relationShip = new Relationship(edge);
            assert Objects.equals(relationShip.getSrcId(), "Tom");
            assert Objects.equals(relationShip.getDstId(), "Lily");
            assert Objects.equals(relationShip.getEdgeName(), "classmate");
            assert relationShip.getRanking() == 100;
            List<String> names = Arrays.asList("prop0", "prop1", "prop2", "prop3", "prop4");
            assert Objects.equals(
                    relationShip.getPropNames().stream().sorted().collect(Collectors.toList()),
                    names.stream().sorted().collect(Collectors.toList()));
            List<Value> values = Arrays.asList(new Value(Value.IVAL, new Long(0)),
                    new Value(Value.IVAL, new Long(1)),
                    new Value(Value.IVAL, new Long(2)),
                    new Value(Value.IVAL, new Long(3)),
                    new Value(Value.IVAL, new Long(4)));
            // TODO: Check the List<Value>
            assert Objects.equals(relationShip.getPropValue("prop1"),
                                  new Value(Value.IVAL, new Long(1)));
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testPathWarpper() {
        try {
            Path path = getPath("Tom", 5);
            PathWrapper pathWrapper = new PathWrapper(path);
            assert pathWrapper.length() == 5;
            Node node = new Node(getVertex("Tom"));
            assert pathWrapper.containNode(node);
            Relationship relationShip = new Relationship(getEdge("Tom", "vertex0"));
            assert pathWrapper.containRelationShip(relationShip);
            List<Node> nodes = new ArrayList<>();
            nodes.add(node);
            for (int i = 0; i < 5; i++) {
                nodes.add(new Node(getVertex(String.format("vertex%d", i))));
            }

            List<Relationship> relationships = new ArrayList<>();
            relationships.add(new Relationship(getEdge("Tom", "vertex0")));
            for (int i = 0; i < 4; i++) {
                if (i % 2 == 0) {
                    relationships.add(new Relationship(getEdge(String.format("vertex%d", i + 1),
                                                               String.format("vertex%d", i))));
                } else {
                    relationships.add(
                            new Relationship(getEdge(String.format("vertex%d", i),
                                             String.format("vertex%d", i + 1))));
                }
            }

            assert Objects.equals(nodes, pathWrapper.getNodes());
            assert Objects.equals(relationships, pathWrapper.getRelationships());
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testResult() {
        try {
            ExecutionResponse resp =  new ExecutionResponse();
            resp.error_code = ErrorCode.SUCCEEDED;
            resp.error_msg = "test".getBytes();
            resp.comment = "test_comment".getBytes();
            resp.latency_in_us = 1000;
            resp.plan_desc = new PlanDescription();
            resp.space_name = "test_space".getBytes();
            resp.data = getDateset();
            ResultSet resultSet = new ResultSet(resp);
            assert resultSet.isSucceeded();
            assert resultSet.getErrorCode() == ErrorCode.SUCCEEDED;
            List<String> expectColNames = Arrays.asList(
                    "col0_int", "col1_string", "col2_vertex", "col3_edge", "col4_path");
            assert Objects.equals(resultSet.getColumnNames(), expectColNames);
            assert resultSet.getRecords().size() == 1;
            assert Objects.equals(new Long(1),
                    resultSet.getRecords().get(0).get(0).asLong());
            assert Objects.equals(new Long(1),
                    resultSet.getRecords().get(0).get("col0_int").asLong());
            System.out.println(String.format("getSVal() = %s",
                    new String(resultSet.getRecords().get(0).get(1).asString())));
            assert Objects.equals("value1", resultSet.getRecords().get(0).get(1).asString());
            assert Objects.equals("value1",
                    resultSet.getRecords().get(0).get("col1_string").asString());

            assert Objects.equals(new Node(getVertex("Tom")),
                    resultSet.getRecords().get(0).get(2).asNode());
            assert Objects.equals(new Node(getVertex("Tom")),
                    resultSet.getRecords().get(0).get("col2_vertex").asNode());

            assert Objects.equals(new Relationship(getEdge("Tom", "Lily")),
                    resultSet.getRecords().get(0).get(3).asRelationShip());
            assert Objects.equals(new Relationship(getEdge("Tom", "Lily")),
                    resultSet.getRecords().get(0).get("col3_edge").asRelationShip());

            assert Objects.equals(new PathWrapper(getPath("Tom", 3)),
                    resultSet.getRecords().get(0).get(4).asPath());
            assert Objects.equals(new PathWrapper(getPath("Tom", 3)),
                    resultSet.getRecords().get(0).get("col4_path").asPath());
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }
}
