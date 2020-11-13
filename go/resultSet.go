/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"fmt"
	"reflect"

	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"

	"github.com/vesoft-inc/nebula-clients/go/nebula"
)

type pair struct {
	propName  string
	propValue *nebula.Value
}

type ResultSet struct {
	columnNames []string
	records     []Record
}

type Record struct {
	columnNames []string
	row         *nebula.Row
}

type Node struct {
	vid           string
	labels        []string
	tagPropKeys   map[string][]string
	tagPropValues map[string][]*nebula.Value
}

type Relationship struct {
	srcVertexID string
	dstVertexID string
	edgeType    nebula.EdgeType
	edgeName    string
	ranking     int64
	properties  *map[string]*nebula.Value
	keys        []string
	values      []*nebula.Value
}

type segment struct {
	startNode    Node
	relationship Relationship
	endNode      Node
}

type PathWrapper struct {
	vertexList       []nebula.Vertex
	relationshipList []Relationship
	segments         []segment
}

func newResultSet(resp graph.ExecutionResponse) ResultSet {
	var records []Record
	var colNames []string
	for _, name := range resp.Data.ColumnNames {
		colNames = append(colNames, string(name))
	}
	for _, row := range resp.Data.Rows {
		records = append(records, Record{
			columnNames: colNames,
			row:         row,
		})
	}
	return ResultSet{
		columnNames: colNames,
		records:     records,
	}
}

func newNode(vertex nebula.Vertex) *Node {
	vid := string(vertex.Vid)
	var (
		keys   []string
		values []*nebula.Value
		labels []string
	)
	tagPropKeys := make(map[string][]string)
	tagPropValues := make(map[string][]*nebula.Value)
	// Iterate through all tags of the vertex
	for _, tag := range vertex.GetTags() {
		name := string(tag.Name)

		for key, value := range tag.GetProps() {
			// Get key and value
			keys = append(keys, key)
			values = append(values, value)
		}
		// Get labels
		labels = append(labels, name)
		// Store key list
		tagPropKeys[name] = keys
		// Store value list
		tagPropValues[name] = values

		keys = nil
		values = nil
	}

	return &Node{
		vid:           vid,
		labels:        labels,
		tagPropKeys:   tagPropKeys,
		tagPropValues: tagPropValues,
	}
}

func newRelationship(edge nebula.Edge) *Relationship {
	srcVertexID := string(edge.GetSrc())
	dstVertexID := string(edge.GetDst())
	edgeType := edge.GetType()
	name := string(edge.Name)
	ranking := int64(edge.Ranking)
	properties := edge.GetProps()

	var (
		keys   []string
		values []*nebula.Value
	)
	// Iterate through all properties of the vertex
	for key, value := range edge.GetProps() {
		// Get key
		keys = append(keys, key)
		// Get value
		values = append(values, value)
	}

	return &Relationship{
		srcVertexID: srcVertexID,
		dstVertexID: dstVertexID,
		edgeType:    edgeType,
		edgeName:    name,
		ranking:     ranking,
		properties:  &properties,
		keys:        keys,
		values:      values,
	}
}

/*
	Return a Node if the value at key is a vertex.
	Key is column name being look up if key is a String.
	If key is an int, key is the index of row.
*/
func (record Record) AsNode(key interface{}) (*Node, error) {
	// Get vertex by column name
	if reflect.TypeOf(key).String() == "string" {
		// Get index
		index, err := record.getIndexbyColName(key.(string))
		if err != nil {
			return nil, err
		}
		return record.getNodeByIndex(index)
	}
	// Get vertex by row index
	if reflect.TypeOf(key).String() == "int" {
		index := key.(int)
		return record.getNodeByIndex(index)
	}
	return nil, fmt.Errorf("Failed to get node: requested coloumn name or index is invalid")
}

func (record Record) getIndexbyColName(colName string) (int, error) {
	index := 0
	for _, name := range record.columnNames {
		if colName == name {
			return index, nil
		}
		index++
	}
	return -1, fmt.Errorf("Column name does not exists")
}

// Return a node if the value at the given index of row is a Vertex
func (record Record) getNodeByIndex(index int) (*Node, error) {
	// Check if the value is a vertex
	if !isVertex(record.row.Values[index]) {
		return nil, fmt.Errorf("Type Error: Value being checked is not a vertex")
	}
	vertex := record.row.Values[index].VVal
	return newNode(*vertex), nil
}

// Return a list of labels of node
func (node Node) GetID() string {
	return node.vid
}

// Return a list of labels of node
func (node Node) Labels() []string {
	return node.labels
}

// Check if node contains given label
func (node Node) HasLabel(l string) bool {
	for _, label := range node.labels {
		if label == l {
			return true
		}
	}
	return false
}

type propertyKV interface {
	Properties() map[string]*nebula.Value
	Keys() []string
	Values() []*nebula.Value
}

// Return all properties of a tag
func (node Node) Properties(tagName string) (map[string]*nebula.Value, error) {
	kvMap := make(map[string]*nebula.Value)
	keys, err := node.Keys(tagName)
	if err != nil {
		return nil, err
	}
	values, err := node.Values(tagName)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(keys); i++ {
		kvMap[keys[i]] = values[i]
	}
	return kvMap, nil
}

func (node Node) Keys(tagName string) ([]string, error) {
	if keys, ok := node.tagPropKeys[tagName]; ok {
		return keys, nil
	}
	return nil, fmt.Errorf("Invalid tag name")
}

func (node Node) Values(tagName string) ([]*nebula.Value, error) {
	if values, ok := node.tagPropValues[tagName]; ok {
		return values, nil
	}
	return nil, fmt.Errorf("Invalid tag name")
}

func (record Record) AsRelationship(key interface{}) (*Relationship, error) {
	// Get vertex by column name
	if reflect.TypeOf(key).String() == "string" {
		// Get index
		index, err := record.getIndexbyColName(key.(string))
		if err != nil {
			return nil, err
		}
		return record.getRelationshipByIndex(index)
	}
	// Get vertex by row index
	if reflect.TypeOf(key).String() == "int" {
		index := key.(int)
		return record.getRelationshipByIndex(index)
	}
	return nil, fmt.Errorf("Failed to get relationship: requested coloumn name or index is invalid")
}

func (record Record) getRelationshipByIndex(index int) (*Relationship, error) {
	// Check if the value is an edge
	if !isVertex(record.row.Values[index]) {
		return nil, fmt.Errorf("Type Error: Value being checked is not an edge")
	}

	edge := record.row.Values[index].EVal
	return newRelationship(*edge), nil
}

func (relationship Relationship) HasType(t string) bool {
	if relationship.edgeName == t {
		return true
	}
	return false
}

func (relationship Relationship) GetSrcVertexID() string {
	return relationship.srcVertexID
}

func (relationship Relationship) GetDstVertexID() string {
	return relationship.dstVertexID
}

func (relationship Relationship) GetEdgeName() string {
	return relationship.edgeName
}

func (relationship Relationship) GetRanking() int64 {
	return relationship.ranking
}

func (relationship Relationship) GetType() string {
	return string(relationship.edgeType)
}

func (relationship Relationship) Properties() map[string]*nebula.Value {
	return *relationship.properties
}

func (relationship Relationship) Keys() []string {
	return relationship.keys
}

func (relationship Relationship) Values() []*nebula.Value {
	return relationship.values
}

func (record Record) AsPath(key interface{}) (*PathWrapper, error) {
	// Get vertex by column name
	if reflect.TypeOf(key).String() == "string" {
		// Get index
		index, err := record.getIndexbyColName(key.(string))
		if err != nil {
			return nil, err
		}
		return record.getPathByIndex(index)
	}
	// Get vertex by row index
	if reflect.TypeOf(key).String() == "int" {
		index := key.(int)
		return record.getPathByIndex(index)
	}
	return nil, fmt.Errorf("Failed to get path: requested coloumn name or index is invalid")
}

func (record Record) getPathByIndex(index int) (*PathWrapper, error) {
	// Check if the value is a path
	if !isPath(record.row.Values[index]) {
		return nil, fmt.Errorf("Type Error: Value being checked is not an edge")
	}
	var (
		vertexList       []nebula.Vertex
		relationshipList []Relationship
		segList          []segment
		edge             nebula.Edge
	)
	src := record.row.Values[index].PVal.Src
	vertexList = append(vertexList, *src)

	for _, step := range record.row.Values[index].PVal.Steps {
		dst := step.Dst
		vertexList = append(vertexList, *dst)
		// determine direction
		stepType := step.Type
		if stepType > 0 {
			edge = nebula.Edge{
				Src:     src.Vid,
				Dst:     dst.Vid,
				Type:    stepType,
				Name:    step.Name,
				Ranking: step.Ranking,
				Props:   step.Props,
			}
		} else {
			edge = nebula.Edge{
				Src:     dst.Vid, // switch with dst
				Dst:     src.Vid,
				Type:    -stepType,
				Name:    step.Name,
				Ranking: step.Ranking,
				Props:   step.Props,
			}
		}
		relationship := newRelationship(edge)
		relationshipList = append(relationshipList, *relationship)

		segList = append(segList, segment{
			startNode:    *newNode(*src),
			relationship: *relationship,
			endNode:      *newNode(*dst),
		})
		src = dst
	}
	return &PathWrapper{
		vertexList:       vertexList,
		relationshipList: relationshipList,
		segments:         segList,
	}, nil
}

func (path *PathWrapper) GetPathLength() int {
	return len(path.vertexList) - 1
}

func (path *PathWrapper) ContainsVertex(id nebula.VertexID) bool {
	for _, v := range path.vertexList {
		if string(v.Vid) == string(id) {
			return true
		}
	}
	return false
}

func (path *PathWrapper) ContainsRelationship(relationship Relationship) bool {
	for _, r := range path.relationshipList {
		if isEqualRelationship(r, relationship) {
			return true
		}
	}
	return false
}

func (path *PathWrapper) GetStartNode() Node {
	return path.segments[0].startNode
}

func (path *PathWrapper) GetEndNode() Node {
	return path.segments[len(path.segments)-1].endNode
}

func isVertex(value *nebula.Value) bool {
	if value.IsSetVVal() {
		return true
	}
	return false
}

func isEdge(value *nebula.Value) bool {
	if value.IsSetEVal() {
		return true
	}
	return false
}

func isPath(value *nebula.Value) bool {
	if value.IsSetPVal() {
		return true
	}
	return false
}

func isEqualRelationship(r1 Relationship, r2 Relationship) bool {
	if r1.srcVertexID == r2.srcVertexID && r1.dstVertexID == r2.dstVertexID && r1.edgeType == r2.edgeType &&
		r1.edgeName == r2.edgeName && r1.ranking == r2.ranking {
		return true
	}
	return false
}
