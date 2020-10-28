/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package conf

import (
	"log"
	"time"
)

type PoolConfig struct {
	// Socket timeout and Socket connection timeout, unit: seconds
	TimeOut time.Duration
	// The idleTime of the connection, unit: seconds
	// If connection's idle time is longer than idleTime, it will be delete
	// 0 value means the connection will not expire
	IdleTime time.Duration
	// The max connections in pool for all addresses
	MaxConnPoolSize int
	// The min connections in pool for all addresses
	MinConnPoolSize int
}

// Create a new poolConfig object with given parameters
func NewPoolConf(TimeOut time.Duration, IdleTime time.Duration, MaxConnPoolSize int,
	MinConnPoolSize int) PoolConfig {

	var newPoolConfig = PoolConfig{
		TimeOut:         0 * time.Millisecond,
		IdleTime:        0 * time.Millisecond,
		MaxConnPoolSize: 10,
		MinConnPoolSize: 0,
	}
	if TimeOut < 0 {
		log.Printf("Illegal Timeout value")
	}
	newPoolConfig.TimeOut = TimeOut
	if IdleTime < 0 {
		log.Printf("Invalid IdleTime value")
	}
	newPoolConfig.IdleTime = IdleTime
	if MaxConnPoolSize < 1 {
		log.Printf("Invalid MaxConnPoolSize value: %d", MaxConnPoolSize)
	}
	newPoolConfig.MaxConnPoolSize = MaxConnPoolSize
	if MinConnPoolSize < 0 {
		log.Printf("Invalid MinConnPoolSize value: %d", MinConnPoolSize)
	}
	newPoolConfig.MinConnPoolSize = MinConnPoolSize
	return newPoolConfig
}

// Return the default config
func GetDefaultConf() PoolConfig {
	var newPoolConfig = PoolConfig{
		TimeOut:         0 * time.Millisecond,
		IdleTime:        0 * time.Millisecond,
		MaxConnPoolSize: 10,
		MinConnPoolSize: 0,
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
