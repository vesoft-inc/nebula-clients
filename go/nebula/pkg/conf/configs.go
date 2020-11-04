/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package conf

import (
	logger "github.com/vesoft-inc/nebula-clients/go/nebula/pkg/logger"

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
	MinConnPoolSize int, log logger.DefaultLogger) PoolConfig {

	var newPoolConfig = PoolConfig{
		TimeOut:         0 * time.Millisecond,
		IdleTime:        0 * time.Millisecond,
		MaxConnPoolSize: 10,
		MinConnPoolSize: 0,
	}
	if TimeOut < 0 {
		newPoolConfig.TimeOut = 0 * time.Millisecond
		log.Warn("Illegal Timeout value, the default value of 0 second has been applied")
	} else {
		newPoolConfig.TimeOut = TimeOut
	}
	if IdleTime < 0 {
		newPoolConfig.IdleTime = 0 * time.Millisecond
		log.Warn("Invalid IdleTime value, the default value of 0 second has been applied")
	} else {
		newPoolConfig.IdleTime = IdleTime
	}
	if MaxConnPoolSize < 1 {
		newPoolConfig.MaxConnPoolSize = 10
		log.Warn("Invalid MaxConnPoolSize value, the default value of 10 has been applied")
	} else {
		newPoolConfig.MaxConnPoolSize = MaxConnPoolSize
	}
	if MinConnPoolSize < 0 {
		newPoolConfig.MinConnPoolSize = 0
		log.Warn("Invalid MinConnPoolSize value, the default value of 0 has been applied")
	} else {
		newPoolConfig.MinConnPoolSize = MinConnPoolSize
	}

	return newPoolConfig
}

// Return the default config
func GetDefaultConf(log logger.DefaultLogger) PoolConfig {
	var newPoolConfig = PoolConfig{
		TimeOut:         0 * time.Millisecond,
		IdleTime:        0 * time.Millisecond,
		MaxConnPoolSize: 10,
		MinConnPoolSize: 0,
	}
	log.Info("Default configuration loadded")
	return newPoolConfig
}
