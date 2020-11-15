/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"github.com/vesoft-inc/nebula-clients/go/nebula"
)

func ConvertValue(value *nebula.Value) interface{} {
	if value.IsSetNVal() {
		return value.GetNVal()
	} else if value.IsSetBVal() {
		return value.GetBVal()
	} else if value.IsSetIVal() {
		return value.GetIVal()
	} else if value.IsSetFVal() {
		return value.GetFVal()
	} else if value.IsSetSVal() {
		return string(value.GetSVal())
	} else if value.IsSetDVal() {
		return value.GetDVal()
	} else if value.IsSetTVal() {
		return value.GetTVal()
	} else if value.IsSetDtVal() {
		return value.GetDtVal()
	} else if value.IsSetVVal() {
		return value.GetVVal()
	} else if value.IsSetEVal() {
		return value.GetEVal()
	} else if value.IsSetPVal() {
		return value.GetPVal()
	} else if value.IsSetLVal() {
		return value.GetLVal()
	} else if value.IsSetMVal() {
		return value.GetMVal()
	} else if value.IsSetUVal() {
		return value.GetUVal()
	}
	return nil
}
