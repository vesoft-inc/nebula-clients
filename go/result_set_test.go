/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"fmt"
	"sort"
	"testing"

	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"

	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-clients/go/nebula"
)

func TestNode(t *testing.T) {
	vertex := getVertex("Tom")
	node := newNode(vertex)

	assert.Equal(t, "Tom", node.GetID())
	assert.Equal(t, true, node.HasLabel("tag1"))
	assert.Equal(t, []string{"tag0", "tag1", "tag2"}, node.Labels())
	keys, _ := node.Keys("tag1")
	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	sort.Strings(keysCopy)
	assert.Equal(t, []string{"prop0", "prop1", "prop2", "prop3", "prop4"}, keysCopy)
	props, _ := node.Properties("tag1")
	for i := 0; i < len(keysCopy); i++ {
		assert.Equal(t, int64(i), *props[keysCopy[i]].IVal)
	}
}

func TestRelationship(t *testing.T) {
	edge := getEdge("Tom", "Lily")
	relationship := newRelationship(edge)

	assert.Equal(t, "Tom", relationship.GetSrcVertexID())
	assert.Equal(t, "Lily", relationship.GetDstVertexID())
	assert.Equal(t, "classmate", relationship.GetEdgeName())
	assert.Equal(t, int64(100), relationship.GetRanking())
	keys := relationship.Keys()
	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	sort.Strings(keysCopy)
	assert.Equal(t, []string{"prop0", "prop1", "prop2", "prop3", "prop4"}, keysCopy)
	props := relationship.Properties()
	for i := 0; i < len(keysCopy); i++ {
		assert.Equal(t, int64(i), *props[keysCopy[i]].IVal)
	}
}

func TestPathWrapper(t *testing.T) {
	path := getPath("Tom", 5)
	pathWrapper := newPathWrapper(path)
	assert.Equal(t, 5, pathWrapper.GetPathLength())
	node := newNode(getVertex("Tom"))
	assert.Equal(t, true, pathWrapper.ContainsNode(*node))
	relationship := newRelationship(getEdge("Tom", "vertex0"))
	assert.Equal(t, true, pathWrapper.ContainsRelationship(*relationship))

	var nodeList []Node
	nodeList = append(nodeList, *node)
	for i := 0; i < 5; i++ {
		newNode := newNode(getVertex(fmt.Sprintf("vertex%d", i)))
		nodeList = append(nodeList, *newNode)
	}

	var relationshipList []*Relationship
	relationshipList = append(relationshipList, newRelationship(getEdge("Tom", "vertex0")))
	for i := 0; i < 4; i++ {
		var edge *nebula.Edge
		if i%2 == 0 {
			edge = getEdge(fmt.Sprintf("vertex%d", i+1), fmt.Sprintf("vertex%d", i))
		} else {
			edge = getEdge(fmt.Sprintf("vertex%d", i), fmt.Sprintf("vertex%d", i+1))
		}
		relationshipList = append(relationshipList, newRelationship(edge))
	}

	l1 := pathWrapper.GetNodes()
	for i := 0; i < len(nodeList); i++ {
		assert.Equal(t, nodeList[i].GetID(), l1[i].GetID())
	}
	l2 := pathWrapper.GetRelations()
	for i := 0; i < len(relationshipList); i++ {
		assert.Equal(t, true, AreEqualRelationship(*relationshipList[i], l2[i]))
	}
	// Check segments
	segList := pathWrapper.GetSegments()
	srcList := []string{"Tom", "vertex1", "vertex1", "vertex3", "vertex3"}
	dstList := []string{"vertex0", "vertex0", "vertex2", "vertex2", "vertex4"}
	for i := 0; i < len(segList); i++ {
		assert.Equal(t, srcList[i], segList[i].startNode.GetID())
		assert.Equal(t, dstList[i], segList[i].endNode.GetID())
	}
}

func TestDataset(t *testing.T) {
	resp := graph.ExecutionResponse{
		graph.ErrorCode_SUCCEEDED,
		1000,
		getDateset(),
		[]byte("test_space"),
		[]byte("test"),
		graph.NewPlanDescription(),
		[]byte("test_comment")}
	resultSet := newResultSet(resp)
	assert.Equal(t, graph.ErrorCode_SUCCEEDED, resultSet.GetErrorCode())
	assert.Equal(t, true, resultSet.IsSucceed())

	expectedColNames := []string{"col0_int", "col1_string", "col2_vertex", "col3_edge", "col4_path"}
	colNames := resultSet.GetColNames()
	for i := 0; i < len(colNames); i++ {
		assert.Equal(t, expectedColNames[i], colNames[i])
	}

	records := resultSet.GetRecords()
	assert.Equal(t, 1, len(records))
	record := records[0]
	_, err := record.AsNode(0)
	assert.EqualError(t, err, "Type Error: Value being checked is not a vertex")
	_, err = record.AsNode("col2")
	assert.EqualError(t, err, "Column name does not exist")
	_, err = record.AsNode(2.0)
	assert.EqualError(t, err, "Failed to get node: requested coloumn name or index is invalid, the parameter shuold be int or string")
	node, _ := record.AsNode("col2_vertex")
	assert.Equal(t, "Tom", node.GetID())

	// TODO: add more tests
	value, _ := record.GetValue(0)
	assert.Equal(t, int64(1), ConvertValue(value))
	value, _ = record.GetValue(1)
	assert.Equal(t, "value1", ConvertValue(value))
	value, _ = record.GetValue(2)
	assert.Equal(t, getVertex("Tom"), ConvertValue(value))
	value, _ = record.GetValue(3)
	assert.Equal(t, getEdge("Tom", "Lily"), ConvertValue(value))
	value, _ = record.GetValue(4)
	assert.Equal(t, getPath("Tom", 3), ConvertValue(value))
}

func getVertex(vid string) *nebula.Vertex {
	var tags []*nebula.Tag

	for i := 0; i < 3; i++ {
		props := make(map[string]*nebula.Value)
		for j := 0; j < 5; j++ {
			value := setIVal(j)
			key := fmt.Sprintf("prop%d", j)
			props[key] = value
		}
		tag := nebula.Tag{
			Name:  []byte(fmt.Sprintf("tag%d", i)),
			Props: props,
		}
		tags = append(tags, &tag)
	}
	return &nebula.Vertex{
		Vid:  []byte(vid),
		Tags: tags,
	}
}

func getEdge(srcID string, dstID string) *nebula.Edge {
	src := []byte(srcID)
	dst := []byte(dstID)
	props := make(map[string]*nebula.Value)
	for i := 0; i < 5; i++ {
		value := setIVal(i)
		props[fmt.Sprintf("prop%d", i)] = value
	}

	return &nebula.Edge{
		Src:     src,
		Dst:     dst,
		Type:    1,
		Name:    []byte("classmate"),
		Ranking: 100,
		Props:   props,
	}
}

func getPath(startID string, stepNum int) *nebula.Path {
	var steps []*nebula.Step
	for i := 0; i < stepNum; i++ {
		props := make(map[string]*nebula.Value)
		for j := 0; j < 5; j++ {
			value := setIVal(j)
			props[fmt.Sprintf("prop%d", j)] = value
		}
		var edgeType nebula.EdgeType
		edgeType = 1
		if i%2 != 0 {
			edgeType = -1
		}
		dstID := getVertex(fmt.Sprintf("vertex%d", i))
		steps = append(steps, &nebula.Step{
			Dst:     dstID,
			Type:    edgeType,
			Name:    []byte("classmate"),
			Ranking: 100,
			Props:   props,
		})
	}
	start := getVertex(startID)
	return &nebula.Path{
		Src:   start,
		Steps: steps,
	}
}

func getDateset() *nebula.DataSet {
	colNames := [][]byte{
		[]byte("col0_int"),
		[]byte("col1_string"),
		[]byte("col2_vertex"),
		[]byte("col3_edge"),
		[]byte("col4_path"),
	}
	var v1 = nebula.NewValue()
	newNum := new(int64)
	*newNum = int64(1)
	v1.IVal = newNum
	var v2 = nebula.NewValue()
	v2.SVal = []byte("value1")
	var v3 = nebula.NewValue()
	v3.VVal = getVertex("Tom")
	var v4 = nebula.NewValue()
	v4.EVal = getEdge("Tom", "Lily")
	var v5 = nebula.NewValue()
	v5.PVal = getPath("Tom", 3)

	var valueList []*nebula.Value
	valueList = []*nebula.Value{v1, v2, v3, v4, v5}
	var rows []*nebula.Row
	row := &nebula.Row{
		valueList,
	}
	rows = append(rows, row)
	return &nebula.DataSet{
		ColumnNames: colNames,
		Rows:        rows,
	}
}

func setIVal(ival int) *nebula.Value {
	var value = nebula.NewValue()
	newNum := new(int64)
	*newNum = int64(ival)
	value.IVal = newNum
	return value
}
