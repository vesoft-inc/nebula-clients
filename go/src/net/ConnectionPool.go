/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"container/list"
	"errors"
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
	// Process domain to IP
	data.DomainToIP(addresses)

	pool.addresses = addresses
	pool.conf = conf

	// Check input
	if len(addresses) == 0 {
		noAddresErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
		log.Print(noAddresErr)
		return noAddresErr
	}
	if conf == nil {
		noConfErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
		log.Print(noConfErr)
		return noConfErr
	}

	// Create connection to given hosts
	for _, host := range addresses {
		newConn := NewConnection(*host)
		// Open connection to host
		err := newConn.Open(newConn.SeverAddress, *pool.conf)
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
			// Simple round-robin
			newConn := NewConnection(*pool.addresses[i%len(addresses)])
			// Not being used
			newConn.inuse = false
			// Open connection to host
			err := newConn.Open(newConn.SeverAddress, *pool.conf)
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
		log.Println(noAvaliableConnectionErr)
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
	pool.activeConnectionQueue.PushBack(chosenConn)
	for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value == chosenConn {
			pool.idleConnectionQueue.Remove(ele)
		}
	}
	return &newSession, nil
}

func (pool *ConnectionPool) GetIdleConn(curSession *Session) (*Connection, error) {
	// Take an idle connectin is avaliable
	if pool.idleConnectionQueue.Len() > 0 {
		// Update status of server's avaliability
		for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
			if ele.Value.(*Connection).SeverAddress == curSession.connection.SeverAddress {
				ele.Value.(*Connection).SeverAddress.IsAvaliable = false
			}
		}
		// Mark the current host as non-avaliable
		curSession.connection.SeverAddress.IsAvaliable = false
		// Return an idle session that is different from the current one
		for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
			if ele.Value.(*Connection).SeverAddress != curSession.connection.SeverAddress &&
				ele.Value.(*Connection).SeverAddress.IsAvaliable == true {
				return ele.Value.(*Connection), nil
			}
		}
		noAvaliableConnectionErr := errors.New("Failed to reconnect: no avaliable connection to a different host can be found")

		return nil, noAvaliableConnectionErr
	}

	// Create a new connection if there is no idle connection and total connection < pool max size
	totalConn := pool.idleConnectionQueue.Len() + pool.activeConnectionQueue.Len()
	// TODO: use load balencer to decide the host to connect
	var severAddress *data.HostAddress
	severAddress = nil
	// Check if there is valid host in the pool
	if totalConn < pool.conf.MaxConnPoolSize {
		for _, host := range pool.addresses {
			if host.IsAvaliable == true {
				severAddress = host
			}
		}
		// No valid host in the pool
		if severAddress == nil {
			noAvaliableHost := errors.New("Failed to reconnect: no avaliable host in the connection pool")
			log.Print(noAvaliableHost)
			return nil, noAvaliableHost
		}
		newConn := NewConnection(*severAddress)
		newConn.inuse = false

		// Open connection to host
		err := newConn.Open(newConn.SeverAddress, *pool.conf)
		if err != nil {
			log.Printf("Failed to open connection, error: %s \n", err.Error())
			return nil, err
		}

		// Add new connection into idle queue
		pool.idleConnectionQueue.PushBack(newConn)
		return newConn, nil
	}
	// TODO: If no idle avaliable, wait for timeout and reconnect

	// If no idle avaliable and the number of total connection reaches the max pool size, return error/wait for timeout
	noAvaliableConnectionErr := errors.New("Failed to reconnect: no avaliable connection in the connection pool")
	log.Print(noAvaliableConnectionErr)
	return nil, noAvaliableConnectionErr
}

// Release connection to pool
func (pool *ConnectionPool) ReturnObject(session *Session) {
	pool.idleConnectionQueue.PushBack(session.connection)

	for ele := pool.activeConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value == session.connection {
			pool.activeConnectionQueue.Remove(ele)
		}
	}
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
