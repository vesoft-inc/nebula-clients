/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"errors"
	"fmt"
	"log"

	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

var defaultPoolConfigs = conf.DefaultPoolConfig

type RespData struct {
	Resp *graph.ExecutionResponse
	Err  error
}

type ConnectionPool struct {
	session               []*Session
	idleConnectionQueue   []*Connection
	activeConnectionQueue []*Connection
	addresses             []*data.HostAddress
	conf                  *conf.PoolConfig
}

type ConnectionPoolMethod interface {
	initPool(addresses []string, conf conf.PoolConfig)
	getSession(username, password string)
	Execute(stmt string) <-chan RespData
	Close()
}

func (pool *ConnectionPool) InitPool(addresses []*data.HostAddress, conf *conf.PoolConfig) error {
	pool.addresses = addresses
	pool.conf = conf

	// Check input
	if len(addresses) == 0 {
		noAddresErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
		if noAddresErr != nil {
			fmt.Print(noAddresErr)
		}
		return noAddresErr
	}
	if conf == nil {
		noConfErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
		if noConfErr != nil {
			fmt.Print(noConfErr)
		}
		return noConfErr
	}
	// Create connection to given hosts
	for _, host := range addresses {
		newConn := NewConnection(*host)
		// Open connection to host
		err := newConn.Open(newConn.severAddress, *pool.conf)
		if err != nil {
			log.Printf("Failed to open connection, error: %s", err.Error())
			return err
		}
		// Add newly created connection to idle queue
		pool.idleConnectionQueue = append(pool.idleConnectionQueue, &newConn)
	}
	// Create connections to fullfill MinConnPoolSize
	if len(pool.idleConnectionQueue) < pool.conf.MinConnPoolSize {
		connNum := pool.conf.MinConnPoolSize - len(pool.idleConnectionQueue)
		for i := 0; i < connNum; i++ {
			newConn := NewConnection(*pool.addresses[i%len(addresses)])

			// Open connection to host
			err := newConn.Open(newConn.severAddress, *pool.conf)
			if err != nil {
				log.Printf("Failed to open connection, error: %s", err.Error())
				return err
			}
			// Add newly created connection to idle queue
			pool.idleConnectionQueue = append(pool.idleConnectionQueue, &newConn)
		}
	}
	return nil
}

func (pool *ConnectionPool) GetSession(username, password string) (*Session, error) {
	if len(pool.idleConnectionQueue) == 0 {
		noAvaliableConnectionErr := errors.New("Failed to get sessoin: no avaliable connection")
		if noAvaliableConnectionErr != nil {
			fmt.Print(noAvaliableConnectionErr)
		}
		return nil, noAvaliableConnectionErr
	}
	// Authenticate
	chosenConn := *pool.idleConnectionQueue[0]
	resp, err := chosenConn.Authenticate(username, password)
	if err != nil {
		log.Printf("Failed to authenticate with the given credential, error: %s", err.Error())
		return nil, err
	}
	sessionID := resp.GetSessionID()
	// Create new session
	newSession := newSession(sessionID, chosenConn, *pool)
	// Add connction to active queue and pop it from idle queue
	pool.activeConnectionQueue = append(pool.activeConnectionQueue, &chosenConn)
	pool.idleConnectionQueue = pool.idleConnectionQueue[1:]
	return &newSession, nil
}

func (pool *ConnectionPool) GetIdleConn(severAddress data.HostAddress) (*Connection, error) {
	// Take an idle connectin is avaliable
	if len(pool.idleConnectionQueue) > 0 {
		newConn := pool.idleConnectionQueue[0]
		pool.idleConnectionQueue = pool.idleConnectionQueue[1:]
		pool.activeConnectionQueue = append(pool.activeConnectionQueue, newConn)
		return newConn, nil
	}
	// Create a new connection if there is no idle connection and total connection < pool max size
	totalConn := len(pool.idleConnectionQueue) + len(pool.activeConnectionQueue)
	if totalConn < pool.conf.MaxConnPoolSize {
		newConn := NewConnection(severAddress)
		pool.activeConnectionQueue = append(pool.activeConnectionQueue, &newConn)
		return &newConn, nil
	}
	// If no idle avaliable and the number of total connection reaches the max pool size, return error/wait for timeout
	noAvaliableConnectionErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
	if noAvaliableConnectionErr != nil {
		fmt.Print(noAvaliableConnectionErr)
	}
	return nil, noAvaliableConnectionErr
}

// Close all connection
func (pool *ConnectionPool) Close() {
	for _, conn := range pool.activeConnectionQueue {
		conn.Close()
	}
}

func (rsp RespData) String() string {
	if rsp.Err != nil {
		return fmt.Sprintf("Error: %s", rsp.Err.Error())
	}
	return rsp.Resp.String()
}
