/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"fmt"

	"github.com/vesoft-inc/nebula-clients/go/nebula"
)

type ValueWrapper struct {
	value *nebula.Value
}

func (valueWrapper ValueWrapper) IsNull() bool {
	if valueWrapper.value.IsSetNVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsBool() bool {
	if valueWrapper.value.IsSetBVal() {
		return true
	}
	return false
}
func (valueWrapper ValueWrapper) IsEmpty() bool {
	if valueWrapper.value == nil {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsInt() bool {
	if valueWrapper.value.IsSetIVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsFloat() bool {
	if valueWrapper.value.IsSetFVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsString() bool {
	if valueWrapper.value.IsSetSVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsTime() bool {
	if valueWrapper.value.IsSetTVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsDate() bool {
	if valueWrapper.value.IsSetDVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsDateTime() bool {
	if valueWrapper.value.IsSetDtVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsList() bool {
	if valueWrapper.value.IsSetLVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsSet() bool {
	if valueWrapper.value.IsSetSVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsMap() bool {
	if valueWrapper.value.IsSetMVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsVertex() bool {
	if valueWrapper.value.IsSetVVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsEdge() bool {
	if valueWrapper.value.IsSetEVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) IsPath() bool {
	if valueWrapper.value.IsSetPVal() {
		return true
	}
	return false
}

func (valueWrapper ValueWrapper) AsBool() (bool, error) {
	if valueWrapper.value.IsSetBVal() {
		return valueWrapper.value.GetBVal(), nil
	}
	return false, fmt.Errorf("Failed to convert value to bool")
}

func (valueWrapper ValueWrapper) AsInt() (int64, error) {
	if valueWrapper.value.IsSetIVal() {
		return valueWrapper.value.GetIVal(), nil
	}
	return -1, fmt.Errorf("Failed to convert value to int")
}

func (valueWrapper ValueWrapper) AsFloat() (float64, error) {
	if valueWrapper.value.IsSetFVal() {
		return valueWrapper.value.GetFVal(), nil
	}
	return -1, fmt.Errorf("Failed to convert value to float")
}

func (valueWrapper ValueWrapper) AsString() (string, error) {
	if valueWrapper.value.IsSetSVal() {
		return string(valueWrapper.value.GetSVal()), nil
	}
	return "", fmt.Errorf("Failed to convert value to string")
}

// TODO: Need to wrapper TimeWrapper
func (valueWrapper ValueWrapper) AsTime() (*nebula.Time, error) {
	if valueWrapper.value.IsSetTVal() {
		return valueWrapper.value.GetTVal(), nil
	}
	return nil, fmt.Errorf("Failed to convert value to Time")
}

func (valueWrapper ValueWrapper) AsDate() (*nebula.Date, error) {
	if valueWrapper.value.IsSetDVal() {
		return valueWrapper.value.GetDVal(), nil
	}
	return nil, fmt.Errorf("Failed to convert value to Date")
}

func (valueWrapper ValueWrapper) AsDateTime() (*nebula.DateTime, error) {
	if valueWrapper.value.IsSetDtVal() {
		return valueWrapper.value.GetDtVal(), nil
	}
	return nil, fmt.Errorf("Failed to convert value to DateTime")
}

func (valueWrapper ValueWrapper) AsList() ([]ValueWrapper, error) {
	if valueWrapper.value.IsSetLVal() {
		var varList []ValueWrapper
		vals := valueWrapper.value.GetLVal().Values
		for _, val := range vals {
			varList = append(varList, ValueWrapper{val})
		}
		return varList, nil
	}
	return nil, fmt.Errorf("Failed to convert value to List")
}

func (valueWrapper ValueWrapper) AsDedupList() ([]ValueWrapper, error) {
	if valueWrapper.value.IsSetUVal() {
		var varList []ValueWrapper
		vals := valueWrapper.value.GetUVal().Values
		for _, val := range vals {
			varList = append(varList, ValueWrapper{val})
		}
		return varList, nil
	}
	return nil, fmt.Errorf("Failed to convert value to set(deduped list)")
}

func (valueWrapper ValueWrapper) AsMap() (map[string]ValueWrapper, error) {
	if valueWrapper.value.IsSetMVal() {
		newMap := make(map[string]ValueWrapper)

		kvs := valueWrapper.value.GetMVal().Kvs
		for key, val := range kvs {
			newMap[key] = ValueWrapper{val}
		}
		return newMap, nil
	}
	return nil, fmt.Errorf("Failed to convert value to Map")
}

func (valueWrapper ValueWrapper) AsNode() (*Node, error) {
	if !valueWrapper.value.IsSetVVal() {
		return nil, fmt.Errorf("Failed to convert value to Node, value is not an vertex")
	}
	vertex := valueWrapper.value.VVal
	node, err := genNode(vertex)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (valueWrapper ValueWrapper) AsRelationship() (*Relationship, error) {
	if !valueWrapper.value.IsSetEVal() {
		return nil, fmt.Errorf("Failed to convert value to Relationship, value is not an edge")
	}
	edge := valueWrapper.value.EVal
	relationship, err := genRelationship(edge)
	if err != nil {
		return nil, err
	}
	return relationship, nil
}

func (valueWrapper ValueWrapper) AsPath() (*PathWrapper, error) {
	if !valueWrapper.value.IsSetPVal() {
		return nil, fmt.Errorf("Failed to convert value to PathWrapper, value is not an edge")
	}
	path, err := genPathWrapper(valueWrapper.value.PVal)
	if err != nil {
		return nil, err
	}
	return path, nil
}
