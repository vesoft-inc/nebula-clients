/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"fmt"
	"reflect"

	"github.com/vesoft-inc/nebula-clients/go/nebula"
)

type pair struct {
	propName  string
	propValue *nebula.Value
}

type ResultSet struct {
	code         int
	errorMessage string
	columnNames  []string
	records      []Record
}

type Record struct {
	row         nebula.Row
	columnNames []string
}

type Node struct {
	labels    []string
	propertys []pair
	keys      []string
	values    []*nebula.Value
}

type Relationship struct {
}

type Path struct {
}

/*
	Return a Node if the value at key is a vertex.
	Key is column name being look up if key is a String.
	If key is an int, key is the index of row.
*/
func (record Record) asNode(key interface{}) (*Node, error) {
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
	var (
		labels    []string
		propertys []pair
		keys      []string
		values    []*nebula.Value
	)
	// Check if the value is a vertex
	if !isVertex(record.row.Values[index]) {
		return nil, fmt.Errorf("Type Error: Value being checked is not a vertex")
	}
	// Iterate through all tags of the vertex
	for _, tag := range record.row.Values[index].VVal.GetTags() {
		name := string(tag.Name)
		for key, value := range tag.GetProps() {
			propertys = append(propertys, pair{
				propName:  key,
				propValue: value,
			})
			// Get key
			keys = append(keys, key)
			// Get value
			values = append(values, value)
		}
		// Get lables
		labels = append(labels, name)
	}

	return &Node{
		labels:    labels,
		propertys: propertys,
		keys:      keys,
		values:    values,
	}, nil
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

func (node Node) Propertys() []pair {
	return node.propertys
}

func (node Node) Keys() []string {
	return node.keys
}

func (node Node) Values() []*nebula.Value {
	return node.values
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
