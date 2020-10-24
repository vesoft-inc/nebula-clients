/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"container/list"
	"errors"
	"fmt"
	"log"

	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

var defaultPoolConfigs = conf.NewPoolConf(0, 0, 0, 0, 0)

type RespData struct {
	Resp *graph.ExecutionResponse
	Err  error
}

type ConnectionPool struct {
	idleConnectionQueue   list.List
	activeConnectionQueue list.List
	addresses             []*data.HostAddress
	conf                  *conf.PoolConfig
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
			log.Printf("Failed to open connection, error: %s \n", err.Error())
			return err
		}
		// Add newly created connection to idle queue
		pool.idleConnectionQueue.PushBack(newConn)
	}
	// Create connections to fullfill MinConnPoolSize
	if pool.idleConnectionQueue.Len() < pool.conf.MinConnPoolSize {
		connNum := pool.conf.MinConnPoolSize - pool.idleConnectionQueue.Len()
		for i := 0; i < connNum; i++ {
			newConn := NewConnection(*pool.addresses[i%len(addresses)])
			// not being used
			newConn.inuse = false
			// Open connection to host
			err := newConn.Open(newConn.severAddress, *pool.conf)
			if err != nil {
				log.Printf("Failed to open connection, error: %s \n", err.Error())
				return err
			}
			// Add newly created connection to idle queue
			pool.idleConnectionQueue.PushBack(newConn)
		}
	}
	return nil
}

func (pool *ConnectionPool) GetSession(username, password string) (*Session, error) {
	if pool.idleConnectionQueue.Len() == 0 {
		noAvaliableConnectionErr := errors.New("Failed to get sessoin: no avaliable connection")
		if noAvaliableConnectionErr != nil {
			fmt.Println(noAvaliableConnectionErr)
		}
		return nil, noAvaliableConnectionErr
	}
	// Authenticate
	chosenConn := pool.idleConnectionQueue.Front().Value.(*Connection)
	resp, err := chosenConn.Authenticate(username, password)
	if err != nil {
		log.Printf("Failed to authenticate with the given credential, error: %s", err.Error())
		return nil, err
	}
	sessionID := resp.GetSessionID()
	// Create new session
	newSession := newSession(sessionID, chosenConn, pool)

	// Add connction to active queue and pop it from idle queue
	newSession.connPool.activeConnectionQueue.PushBack(chosenConn)
	for ele := newSession.connPool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value == chosenConn {
			newSession.connPool.idleConnectionQueue.Remove(ele)
		}
	}

	return &newSession, nil
}

func (pool *ConnectionPool) GetIdleConn() (*Connection, error) {
	// Take an idle connectin is avaliable
	if pool.idleConnectionQueue.Len() > 0 {
		// TODO add timeout

		newConn := pool.idleConnectionQueue.Front().Value.(*Connection)
		// Pop connection from queue
		// pool.idleConnectionQueue.Remove(pool.idleConnectionQueue.Front())
		// pool.activeConnectionQueue.PushBack(newConn)
		return newConn, nil
	}

	// Create a new connection if there is no idle connection and total connection < pool max size
	totalConn := pool.idleConnectionQueue.Len() + pool.activeConnectionQueue.Len()
	// TODO: use load balencer to decide the host to connect
	severAddress := pool.addresses[0]
	if totalConn < pool.conf.MaxConnPoolSize {
		newConn := NewConnection(*severAddress)
		newConn.inuse = false
		pool.idleConnectionQueue.PushBack(newConn)
		return newConn, nil
	}
	// If no idle avaliable, wait for timeout and reconnect

	// If no idle avaliable and the number of total connection reaches the max pool size, return error/wait for timeout
	noAvaliableConnectionErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
	if noAvaliableConnectionErr != nil {
		fmt.Print(noAvaliableConnectionErr)
	}
	return nil, noAvaliableConnectionErr
}

// Close all connection
func (pool *ConnectionPool) Close() error {
	for conn := pool.idleConnectionQueue.Front(); conn != nil; conn = conn.Next() {
		err := conn.Value.(*Connection).Close()
		if err != nil {
			log.Printf("Failed to close connection, error: %s", err.Error())
			return err
		}
	}
	for conn := pool.activeConnectionQueue.Front(); conn != nil; conn = conn.Next() {
		err := conn.Value.(*Connection).Close()
		if err != nil {
			log.Printf("Failed to close connection, error: %s", err.Error())
			return err
		}
	}
	return nil
}

func (rsp RespData) String() string {
	if rsp.Err != nil {
		return fmt.Sprintf("Error: %s", rsp.Err.Error())
	}
	return rsp.Resp.String()
}
