/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package conf

import (
	"time"
)

type GraphConfig struct {
	TimeOut         time.Duration
	IdleTime        time.Duration
	MaxConnPoolSize int
	MinConnPoolSize int
	MaxRetryTimes   int
}

func (opt *GraphConfig) SetDefualt() {
	opt.TimeOut = 0 * time.Second
}

type PoolConfig struct {
	// Socket timeout and Socket connection timeout, unit: seconds
	TimeOut time.Duration
	// The idleTime of the connection, unit: seconds
	// If connection's idle time is longer than idleTime, it will be delete
	IdleTime time.Duration
	// The man connections in pool for all addresses
	MaxConnPoolSize int
	// The min connections in pool for all addresses
	MinConnPoolSize int
	// the times to retry to connect
	MaxRetryTimes int
}

var DefaultPoolConfig = PoolConfig{
	TimeOut:         1000 * time.Second,
	IdleTime:        0 * time.Second,
	MaxConnPoolSize: 100,
	MinConnPoolSize: 0,
	MaxRetryTimes:   3,
}

func (pool *PoolConfig) GetMinConnPoolSize() int {
	return pool.MinConnPoolSize
}

func (pool *PoolConfig) SetMinConnPoolSize(size int) {
	pool.MinConnPoolSize = size
}

func (pool *PoolConfig) GetMaxConnPoolSize() int {
	return pool.MaxConnPoolSize
}

func (pool *PoolConfig) SetMaxConnPoolSize(size int) {
	pool.MaxConnPoolSize = size
}

func (pool *PoolConfig) GetTimeOut() time.Duration {
	return pool.TimeOut
}

func (pool *PoolConfig) SetTimeOut(duration time.Duration) {
	pool.TimeOut = duration
}

func (pool *PoolConfig) GetIdleTime() time.Duration {
	return pool.IdleTime
}

func (pool *PoolConfig) SetIdleTime(duration time.Duration) {
	pool.IdleTime = duration
}

func (pool *PoolConfig) GetMaxRetryTimes() int {
	return pool.MaxRetryTimes
}

func (pool *PoolConfig) SetMaxRetryTimes(times int) {
	pool.MaxRetryTimes = times
}
