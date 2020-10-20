/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package conf

import (
	"time"
)

type GraphOptions struct {
	Timeout                  time.Duration
	Idle_time                time.Duration
	Max_connection_pool_size int
	Min_connection_pool_size int
	Max_retry_times          int
}
