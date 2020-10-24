/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package conf

import (
	"time"
)

// type GraphConfig struct {
// 	TimeOut         time.Duration
// 	IdleTime        time.Duration
// 	MaxConnPoolSize int
// 	MinConnPoolSize int
// 	MaxRetryTimes   int
// }

// func (opt *GraphConfig) SetDefualt() {
// 	opt.TimeOut = 0 * time.Second
// }

type PoolConfig struct {
	// Socket timeout and Socket connection timeout, unit: seconds
	TimeOut time.Duration
	// The idleTime of the connection, unit: seconds
	// If connection's idle time is longer than idleTime, it will be delete
	IdleTime time.Duration
	// The max connections in pool for all addresses
	MaxConnPoolSize int
	// The min connections in pool for all addresses
	MinConnPoolSize int
	// the times to retry to connect
	MaxRetryTimes int
}

// Use 0 as parameter to get the default configs
func NewPoolConf(TimeOut time.Duration, IdleTime time.Duration, MaxConnPoolSize int,
	MinConnPoolSize int, MaxRetryTimes int) PoolConfig {

	var newPoolConfig = PoolConfig{
		TimeOut:         1000 * time.Millisecond,
		IdleTime:        5 * 60 * 1000 * time.Millisecond,
		MaxConnPoolSize: 100,
		MinConnPoolSize: 0,
		MaxRetryTimes:   3,
	}
	if TimeOut != 0 {
		newPoolConfig.TimeOut = TimeOut
	}
	if IdleTime != 0 {
		newPoolConfig.IdleTime = IdleTime
	}
	if MaxConnPoolSize != 0 {
		newPoolConfig.MaxConnPoolSize = MaxConnPoolSize
	}
	if MinConnPoolSize != 0 {
		newPoolConfig.MinConnPoolSize = MinConnPoolSize
	}
	if MaxRetryTimes != 0 {
		newPoolConfig.MaxRetryTimes = MaxRetryTimes
	}
	return newPoolConfig
}

func (config *PoolConfig) GetMinConnPoolSize() int {
	return config.MinConnPoolSize
}

func (config *PoolConfig) SetMinConnPoolSize(size int) {
	config.MinConnPoolSize = size
}

func (config *PoolConfig) GetMaxConnPoolSize() int {
	return config.MaxConnPoolSize
}

func (config *PoolConfig) SetMaxConnPoolSize(size int) {
	config.MaxConnPoolSize = size
}

func (config *PoolConfig) GetTimeOut() time.Duration {
	return config.TimeOut
}

func (config *PoolConfig) SetTimeOut(duration time.Duration) {
	config.TimeOut = duration
}

func (config *PoolConfig) GetIdleTime() time.Duration {
	return config.IdleTime
}

func (config *PoolConfig) SetIdleTime(duration time.Duration) {
	config.IdleTime = duration
}

func (config *PoolConfig) GetMaxRetryTimes() int {
	return config.MaxRetryTimes
}

func (config *PoolConfig) SetMaxRetryTimes(times int) {
	config.MaxRetryTimes = times
}
