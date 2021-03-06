/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vesoft-inc/nebula-clients/go/nebula"
	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

type ResultSet struct {
	resp            *graph.ExecutionResponse
	columnNames     []string
	colNameIndexMap map[string]int
}

type Record struct {
	columnNames     *[]string
	_record         []*ValueWrapper
	colNameIndexMap *map[string]int
}

type Node struct {
	vertex          *nebula.Vertex
	tags            []string // tag name
	tagNameIndexMap map[string]int
}

type Relationship struct {
	edge *nebula.Edge
}

type segment struct {
	startNode    *Node
	relationship *Relationship
	endNode      *Node
}

type PathWrapper struct {
	nodeList         []*Node
	relationshipList []*Relationship
	segments         []segment
}

type ErrorCode int64

const (
	ErrorCode_SUCCEEDED               ErrorCode = ErrorCode(graph.ErrorCode_SUCCEEDED)
	ErrorCode_E_DISCONNECTED          ErrorCode = ErrorCode(graph.ErrorCode_E_DISCONNECTED)
	ErrorCode_E_FAIL_TO_CONNECT       ErrorCode = ErrorCode(graph.ErrorCode_E_FAIL_TO_CONNECT)
	ErrorCode_E_RPC_FAILURE           ErrorCode = ErrorCode(graph.ErrorCode_E_RPC_FAILURE)
	ErrorCode_E_BAD_USERNAME_PASSWORD ErrorCode = ErrorCode(graph.ErrorCode_E_BAD_USERNAME_PASSWORD)
	ErrorCode_E_SESSION_INVALID       ErrorCode = ErrorCode(graph.ErrorCode_E_SESSION_INVALID)
	ErrorCode_E_SESSION_TIMEOUT       ErrorCode = ErrorCode(graph.ErrorCode_E_SESSION_TIMEOUT)
	ErrorCode_E_SYNTAX_ERROR          ErrorCode = ErrorCode(graph.ErrorCode_E_SYNTAX_ERROR)
	ErrorCode_E_EXECUTION_ERROR       ErrorCode = ErrorCode(graph.ErrorCode_E_EXECUTION_ERROR)
	ErrorCode_E_STATEMENT_EMTPY       ErrorCode = ErrorCode(graph.ErrorCode_E_STATEMENT_EMTPY)
	ErrorCode_E_USER_NOT_FOUND        ErrorCode = ErrorCode(graph.ErrorCode_E_USER_NOT_FOUND)
	ErrorCode_E_BAD_PERMISSION        ErrorCode = ErrorCode(graph.ErrorCode_E_BAD_PERMISSION)
	ErrorCode_E_SEMANTIC_ERROR        ErrorCode = ErrorCode(graph.ErrorCode_E_SEMANTIC_ERROR)
)

func genResultSet(resp *graph.ExecutionResponse) *ResultSet {
	var colNames []string
	var colNameIndexMap = make(map[string]int)

	if resp.Data == nil { // if resp.Data != nil then resp.Data.row and resp.Data.colNames wont be nil
		return &ResultSet{
			resp:            resp,
			columnNames:     colNames,
			colNameIndexMap: colNameIndexMap,
		}
	}
	for i, name := range resp.Data.ColumnNames {
		colNames = append(colNames, string(name))
		colNameIndexMap[string(name)] = i
	}

	return &ResultSet{
		resp:            resp,
		columnNames:     colNames,
		colNameIndexMap: colNameIndexMap,
	}
}

func genValWarps(row *nebula.Row) ([]*ValueWrapper, error) {
	if row == nil {
		return nil, fmt.Errorf("Failed to generate valueWrapper: invalid row")
	}
	var valWraps []*ValueWrapper
	for _, val := range row.Values {
		if val == nil {
			return nil, fmt.Errorf("Failed to generate valueWrapper: value is nil")
		}
		valWraps = append(valWraps, &ValueWrapper{val})
	}
	return valWraps, nil
}

func genNode(vertex *nebula.Vertex) (*Node, error) {
	if vertex == nil {
		return nil, fmt.Errorf("Failed to generate Node: invalid vertex")
	}
	var tags []string
	nameIndex := make(map[string]int)

	// Iterate through all tags of the vertex
	for i, tag := range vertex.GetTags() {
		name := string(tag.Name)
		// Get tags
		tags = append(tags, name)
		nameIndex[name] = i
	}

	return &Node{
		vertex:          vertex,
		tags:            tags,
		tagNameIndexMap: nameIndex,
	}, nil
}

func genRelationship(edge *nebula.Edge) (*Relationship, error) {
	if edge == nil {
		return nil, fmt.Errorf("Failed to generate Relationship: invalid edge")
	}
	return &Relationship{
		edge: edge,
	}, nil
}

func genPathWrapper(path *nebula.Path) (*PathWrapper, error) {
	if path == nil {
		return nil, fmt.Errorf("Failed to generate Path Wrapper: invalid path")
	}
	var (
		nodeList         []*Node
		relationshipList []*Relationship
		segList          []segment
		edge             *nebula.Edge
		segStartNode     *Node
		segEndNode       *Node
		segType          nebula.EdgeType
	)
	src, err := genNode(path.Src)
	if err != nil {
		return nil, err
	}
	nodeList = append(nodeList, src)

	for _, step := range path.Steps {
		dst, err := genNode(step.Dst)
		if err != nil {
			return nil, err
		}
		nodeList = append(nodeList, dst)
		// determine direction
		stepType := step.Type
		if stepType > 0 {
			segStartNode = src
			segEndNode = dst
			segType = stepType
		} else {
			segStartNode = dst // switch with src
			segEndNode = src
			segType = -stepType
		}
		edge = &nebula.Edge{
			Src:     []byte(segStartNode.GetID()),
			Dst:     []byte(segEndNode.GetID()),
			Type:    segType,
			Name:    step.Name,
			Ranking: step.Ranking,
			Props:   step.Props,
		}
		relationship, err := genRelationship(edge)
		if err != nil {
			return nil, err
		}
		relationshipList = append(relationshipList, relationship)

		segList = append(segList, segment{
			startNode:    segStartNode,
			relationship: relationship,
			endNode:      segEndNode,
		})
		src = dst
	}
	return &PathWrapper{
		nodeList:         nodeList,
		relationshipList: relationshipList,
		segments:         segList,
	}, nil
}

// Returns ExecutionResponse as a JSON []byte.
// To get the string value in the nested JSON struct, decode with base64
func (res ResultSet) MarshalJSON() ([]byte, error) {
	if res.resp.Data == nil {
		return nil, fmt.Errorf("Failed to generate JSON, DataSet is empty")
	}
	return json.Marshal(res.resp.Data)
}

// Returns a 2D array of strings representing the query result
// If resultSet.resp.data is nil, returns an empty 2D array
func (res ResultSet) AsStringTable() [][]string {
	var resTable [][]string
	colNames := res.GetColNames()
	resTable = append(resTable, colNames)
	rows := res.GetRows()
	for _, row := range rows {
		var tempRow []string
		for _, val := range row.Values {
			tempRow = append(tempRow, ValueWrapper{val}.String())
		}
		resTable = append(resTable, tempRow)
	}
	return resTable
}

// Returns all values in the given column
func (res ResultSet) GetValuesByColName(colName string) ([]*ValueWrapper, error) {
	if !res.hasColName(colName) {
		return nil, fmt.Errorf("Failed to get values, given column name '%s' does not exist", colName)
	}
	// Get index
	index := res.colNameIndexMap[colName]
	var valList []*ValueWrapper
	for _, row := range res.resp.Data.Rows {
		valList = append(valList, &ValueWrapper{row.Values[index]})
	}
	return valList, nil
}

// Returns all values in the row at given index
func (res ResultSet) GetRowValuesByIndex(index int) (*Record, error) {
	if err := checkIndex(index, res.resp.Data.Rows); err != nil {
		return nil, err
	}
	valWrap, err := genValWarps(res.resp.Data.Rows[index])
	if err != nil {
		return nil, err
	}
	return &Record{
		columnNames:     &res.columnNames,
		_record:         valWrap,
		colNameIndexMap: &res.colNameIndexMap,
	}, nil
}

// Returns the number of total rows
func (res ResultSet) GetRowSize() (int, error) {
	if res.resp.Data == nil {
		return -1, fmt.Errorf("Failed to get row size, DataSet is empty")
	}
	return len(res.resp.Data.Rows), nil
}

// Returns the number of total columns
func (res ResultSet) GetColSize() (int, error) {
	if res.resp.Data == nil {
		return -1, fmt.Errorf("Failed to get column size, DataSet is empty")
	}
	return len(res.resp.Data.ColumnNames), nil
}

// Returns all rows
func (res ResultSet) GetRows() []*nebula.Row {
	if res.resp.Data == nil {
		var empty []*nebula.Row
		return empty
	}
	return res.resp.Data.Rows
}

func (res ResultSet) GetColNames() []string {
	return res.columnNames
}

// Returns an integer representing an error type
// 0    ErrorCode_SUCCEEDED
// -1   ErrorCode_E_DISCONNECTED
// -2   ErrorCode_E_FAIL_TO_CONNECT
// -3   ErrorCode_E_RPC_FAILURE
// -4   ErrorCode_E_BAD_USERNAME_PASSWORD
// -5   ErrorCode_E_SESSION_INVALID
// -6   ErrorCode_E_SESSION_TIMEOUT
// -7   ErrorCode_E_SYNTAX_ERROR
// -8   ErrorCode_E_EXECUTION_ERROR
// -9   ErrorCode_E_STATEMENT_EMTPY
// -10  ErrorCode_E_USER_NOT_FOUND
// -11  ErrorCode_E_BAD_PERMISSION
// -12  ErrorCode_E_SEMANTIC_ERROR
func (res ResultSet) GetErrorCode() ErrorCode {
	return ErrorCode(int64(res.resp.ErrorCode))
}

func (res ResultSet) GetLatency() int32 {
	return res.resp.LatencyInUs
}

func (res ResultSet) GetSpaceName() string {
	if res.resp.Comment == nil {
		return ""
	}
	return string(res.resp.SpaceName)
}

func (res ResultSet) GetErrorMsg() string {
	if res.resp.ErrorMsg == nil {
		return ""
	}
	return string(res.resp.ErrorMsg)
}

func (res ResultSet) GetPlanDesc() *graph.PlanDescription {
	if res.resp.Comment == nil {
		return graph.NewPlanDescription()
	}
	return res.resp.PlanDesc
}

func (res ResultSet) GetComment() string {
	if res.resp.Comment == nil {
		return ""
	}
	return string(res.resp.Comment)
}

func (res ResultSet) IsEmpty() bool {
	if res.resp.Data == nil || len(res.resp.Data.ColumnNames) == 0 || len(res.resp.Data.Rows) == 0 {
		return true
	}
	return false
}

func (res ResultSet) IsSucceed() bool {
	return res.GetErrorCode() == ErrorCode_SUCCEEDED
}

func (res ResultSet) hasColName(colName string) bool {
	if _, ok := res.colNameIndexMap[colName]; ok {
		return true
	}
	return false
}

// Returns value in the record at given column index
func (record Record) GetValueByIndex(index int) (*ValueWrapper, error) {
	if err := checkIndex(index, record._record); err != nil {
		return nil, err
	}
	return record._record[index], nil
}

// Returns value in the record at given column name
func (record Record) GetValueByColName(colName string) (*ValueWrapper, error) {
	if !record.hasColName(colName) {
		return nil, fmt.Errorf("Failed to get values, given column name '%s' does not exist", colName)
	}
	// Get index
	index := (*record.colNameIndexMap)[colName]
	return record._record[index], nil
}

func (record Record) PrintRow() {
	var strList []string
	for _, val := range record._record {
		strList = append(strList, val.String())
	}
	fmt.Printf(strings.Join(strList, ", "))
}

func (record Record) hasColName(colName string) bool {
	if _, ok := (*record.colNameIndexMap)[colName]; ok {
		return true
	}
	return false
}

// Returns a list of tags of node
func (node Node) GetID() string {
	return string(node.vertex.GetVid())
}

// Returns a list of tag names of node
func (node Node) GetTags() []string {
	return node.tags
}

// Check if node contains given label
func (node Node) HasTag(label string) bool {
	if _, ok := node.tagNameIndexMap[label]; ok {
		return true
	}
	return false
}

// Returns all properties of a tag
func (node Node) Properties(tagName string) (map[string]*ValueWrapper, error) {
	kvMap := make(map[string]*ValueWrapper)
	// Check if label exists
	if !node.HasTag(tagName) {
		return nil, fmt.Errorf("Failed to get properties: Tag name %s does not exsist in the Node", tagName)
	}
	index := node.tagNameIndexMap[tagName]
	for k, v := range node.vertex.Tags[index].Props {
		kvMap[k] = &ValueWrapper{v}
	}
	return kvMap, nil
}

// Returns all prop names of the given tag name
func (node Node) Keys(tagName string) ([]string, error) {
	if !node.HasTag(tagName) {
		return nil, fmt.Errorf("Failed to get properties: Tag name %s does not exsist in the Node", tagName)
	}
	var propNameList []string
	index := node.tagNameIndexMap[tagName]
	for k, _ := range node.vertex.Tags[index].Props {
		propNameList = append(propNameList, k)
	}
	return propNameList, nil
}

// Returns all prop values of the given tag name
func (node Node) Values(tagName string) ([]*ValueWrapper, error) {
	if !node.HasTag(tagName) {
		return nil, fmt.Errorf("Failed to get properties: Tag name %s does not exsist in the Node", tagName)
	}
	var propValList []*ValueWrapper
	index := node.tagNameIndexMap[tagName]
	for _, v := range node.vertex.Tags[index].Props {
		propValList = append(propValList, &ValueWrapper{v})
	}
	return propValList, nil
}

// Returns true if two nodes have same vid
func (n1 Node) IsEqualTo(n2 *Node) bool {
	return n1.GetID() == n2.GetID()
}

func (relationship Relationship) GetSrcVertexID() string {
	return string(relationship.edge.GetSrc())
}

func (relationship Relationship) GetDstVertexID() string {
	return string(relationship.edge.GetDst())
}

func (relationship Relationship) GetEdgeName() string {
	return string(relationship.edge.Name)
}

func (relationship Relationship) GetRanking() int64 {
	return int64(relationship.edge.Ranking)
}

func (relationship Relationship) Properties() map[string]*ValueWrapper {
	kvMap := make(map[string]*ValueWrapper)
	var (
		keyList   []string
		valueList []*ValueWrapper
	)
	for k, v := range relationship.edge.Props {
		keyList = append(keyList, k)
		valueList = append(valueList, &ValueWrapper{v})
	}

	for i := 0; i < len(keyList); i++ {
		kvMap[keyList[i]] = valueList[i]
	}
	return kvMap
}

// Returns a list of keys
func (relationship Relationship) Keys() []string {
	var keys []string
	for key, _ := range relationship.edge.GetProps() {
		keys = append(keys, key)
	}
	return keys
}

// Returns a list of values wrapped as ValueWrappers
func (relationship Relationship) Values() []*ValueWrapper {
	var values []*ValueWrapper
	for _, value := range relationship.edge.GetProps() {
		values = append(values, &ValueWrapper{value})
	}
	return values
}

func (r1 Relationship) IsEqualTo(r2 *Relationship) bool {
	if string(r1.edge.GetSrc()) == string(r2.edge.GetSrc()) && string(r1.edge.GetDst()) == string(r2.edge.GetDst()) &&
		string(r1.edge.Name) == string(r2.edge.Name) && r1.edge.Ranking == r2.edge.Ranking {
		return true
	}
	return false
}

func (path *PathWrapper) GetPathLength() int {
	return len(path.segments)
}

func (path *PathWrapper) GetNodes() []*Node {
	return path.nodeList
}

func (path *PathWrapper) GetRelationships() []*Relationship {
	return path.relationshipList
}

func (path *PathWrapper) GetSegments() []segment {
	return path.segments
}

func (path *PathWrapper) ContainsNode(node Node) bool {
	for _, n := range path.nodeList {
		if n.GetID() == node.GetID() {
			return true
		}
	}
	return false
}

func (path *PathWrapper) ContainsRelationship(relationship *Relationship) bool {
	for _, r := range path.relationshipList {
		if r.IsEqualTo(relationship) {
			return true
		}
	}
	return false
}

func (path *PathWrapper) GetStartNode() (*Node, error) {
	if len(path.segments) == 0 {
		return nil, fmt.Errorf("Failed to get start node, no node in the path")
	}
	return path.segments[0].startNode, nil
}

func (path *PathWrapper) GetEndNode() (*Node, error) {
	if len(path.segments) == 0 {
		return nil, fmt.Errorf("Failed to get start node, no node in the path")
	}
	return path.segments[len(path.segments)-1].endNode, nil
}

func (p1 *PathWrapper) IsEqualTo(p2 *PathWrapper) bool {
	// Check length
	if len(p1.nodeList) != len(p2.nodeList) || len(p1.relationshipList) != len(p2.relationshipList) ||
		len(p1.segments) != len(p2.segments) {
		return false
	}
	// Check nodes
	for i := 0; i < len(p1.nodeList); i++ {
		if !p1.nodeList[i].IsEqualTo(p2.nodeList[i]) {
			return false
		}
	}
	// Check relationships
	for i := 0; i < len(p1.relationshipList); i++ {
		if !p1.relationshipList[i].IsEqualTo(p2.relationshipList[i]) {
			return false
		}
	}
	// Check segments
	for i := 0; i < len(p1.segments); i++ {
		if !p1.segments[i].startNode.IsEqualTo(p2.segments[i].startNode) ||
			!p1.segments[i].endNode.IsEqualTo(p2.segments[i].endNode) ||
			!p1.segments[i].relationship.IsEqualTo(p2.segments[i].relationship) {
			return false
		}
	}
	return true
}

func checkIndex(index int, list interface{}) error {
	if _, ok := list.([]*nebula.Row); ok {
		if index < 0 || index >= len(list.([]*nebula.Row)) {
			return fmt.Errorf("Failed to get Value, the index is out of range")
		}
		return nil
	} else if _, ok := list.([]*ValueWrapper); ok {
		if index < 0 || index >= len(list.([]*ValueWrapper)) {
			return fmt.Errorf("Failed to get Value, the index is out of range")
		}
		return nil
	}
	return fmt.Errorf("Given list type is invalid")
}
