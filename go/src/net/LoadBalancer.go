/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

// TODO: add implement round-robin rule

package nebulaNet

import (
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type LoadBalancer struct {
	addresses    *data.HostAddress
	serverStatus map[*data.HostAddress]int
}
