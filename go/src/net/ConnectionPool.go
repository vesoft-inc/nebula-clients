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

var defaultPoolConfigs = conf.NewPoolConf(0, 0, 0, 0)

type RespData struct {
	Resp *graph.ExecutionResponse
	Err  error
}

type ConnectionPool struct {
	idleConnectionQueue   list.List
	activeConnectionQueue list.List
	addresses             []*data.HostAddress
	conf                  *conf.PoolConfig
	loadBalancer          *LoadBalancer
	ConnectionInUse       map[*Connection]bool
}

func (pool *ConnectionPool) InitPool(addresses []*data.HostAddress, conf *conf.PoolConfig) error {
	// Process domain to IP
	err := data.DomainToIP(addresses)
	if err != nil {
		log.Printf("Failed to find IP, error: %s \n", err.Error())
		return err
	}

	pool.addresses = addresses
	pool.conf = conf
	pool.loadBalancer = NewLoadbalancer(addresses, pool)
	pool.ConnectionInUse = make(map[*Connection]bool)

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

	for i := 0; i < pool.conf.MinConnPoolSize; i++ {
		// Simple round-robin
		newConn := NewConnection(*pool.addresses[i%len(addresses)])
		// Not being used
		pool.ConnectionInUse[newConn] = false
		// Open connection to host
		err := newConn.Open(newConn.SeverAddress, *pool.conf)
		if err != nil {
			log.Printf("Failed to open connection, error: %s \n", err.Error())
			return err
		}

		// Mark connection as in use
		pool.ConnectionInUse[newConn] = false
		// Update host status
		pool.loadBalancer.ValidateHost(&newConn.SeverAddress)

		// Add newly created connection to idle queue
		pool.idleConnectionQueue.PushBack(newConn)
	}
	return nil
}

func (pool *ConnectionPool) GetSession(username, password string) (*Session, error) {
	if pool.conf == nil {
		notInitialized := errors.New("Failed to get session: There is no config in the connection pool")
		log.Println(notInitialized)
		return nil, notInitialized
	}

	// Get valid and usable connection
	conn, err := pool.GetIdleConn()
	if err != nil {
		log.Printf("Failed to get session, error: %s", err.Error())
		return nil, err
	}

	// Authenticate
	resp, err := conn.Authenticate(username, password)
	if err != nil {
		log.Printf("Failed to authenticate with the given credential, error: %s", err.Error())
		return nil, err
	}

	sessionID := resp.GetSessionID()
	// Create new session
	newSession := newSession(sessionID, conn, pool)
	// Mark connection as in use
	pool.ConnectionInUse[conn] = true
	// Add connction to active queue and pop it from idle queue
	pool.activeConnectionQueue.PushBack(conn)

	pool.ConnectionInUse[conn] = true
	// Update host status
	pool.loadBalancer.ValidateHost(&conn.SeverAddress)

	for _, status := range pool.loadBalancer.ServerStatusList {
		if *status.address == conn.SeverAddress {
			status.workLoad = status.workLoad + 1
		}
	}
	return &newSession, nil

}

func (pool *ConnectionPool) GetIdleConn() (*Connection, error) {
	// Update status of all server's avaliability
	pool.loadBalancer.UpdateServerStatus()

	// Take an idle connection if possible
	if pool.idleConnectionQueue.Len() > 0 {
		pool.UpdateConnectionStatus()
		newConn, err := pool.GetValidConn()
		if err != nil {
			log.Printf("Error: %s \n", err.Error())
			return nil, err
		}
		for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
			if ele.Value.(*Connection) == newConn {
				pool.idleConnectionQueue.Remove(ele)
			}
		}
		return newConn, nil
	}

	// Create a new connection if there is no idle connection and total connection < pool max size
	totalConn := pool.idleConnectionQueue.Len() + pool.activeConnectionQueue.Len()
	if totalConn < pool.conf.MaxConnPoolSize {
		// Get a valid host
		host, err := pool.loadBalancer.GetValidHost()
		// No valid host in the pool
		if err != nil {
			log.Printf("Error: %s \n", err.Error())
			return nil, err
		}
		newConn := NewConnection(*host)

		// Open connection to host
		err = newConn.Open(newConn.SeverAddress, *pool.conf)
		if err != nil {
			log.Printf("Failed to open connection, error: %s \n", err.Error())
			return nil, err
		}
		return newConn, nil
	}
	// TODO: If no idle avaliable, wait for timeout and reconnect

	// If no idle avaliable and the number of total connection reaches the max pool size, return error/wait for timeout
	noAvaliableConnectionErr := errors.New("Failed to get connection: no avaliable connection in the connection pool")
	log.Print(noAvaliableConnectionErr)
	return nil, noAvaliableConnectionErr
}

// Release connection to pool
func (pool *ConnectionPool) ReturnObject(conn *Connection) {
	pool.idleConnectionQueue.PushBack(conn)
	pool.ConnectionInUse[conn] = false

	for ele := pool.activeConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value.(*Connection) == conn {
			pool.activeConnectionQueue.Remove(ele)
		}
	}

	for _, status := range pool.loadBalancer.ServerStatusList {
		if *status.address == conn.SeverAddress {
			status.workLoad = status.workLoad - 1
		}
	}
}

// Close all connection
func (pool *ConnectionPool) Close() {
	for conn := pool.idleConnectionQueue.Front(); conn != nil; conn = conn.Next() {
		conn.Value.(*Connection).Close()
		pool.idleConnectionQueue.Remove(conn)
	}
	for conn := pool.activeConnectionQueue.Front(); conn != nil; conn = conn.Next() {
		conn.Value.(*Connection).Close()
		pool.activeConnectionQueue.Remove(conn)
	}
}

// Ping all connection in the idle queue and update remove unresponsive connection
func (pool *ConnectionPool) UpdateConnectionStatus() {
	for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if _, err := ele.Value.(*Connection).Ping(); err != nil {
			// If the connection is not valid, close it and remove from queue
			ele.Value.(*Connection).Close()
			pool.idleConnectionQueue.Remove(ele)
		}
		ele.Value.(*Connection).ValidateConnection()
	}
}

// Return the first connection in the idle connection queue that has a valid host
func (pool *ConnectionPool) GetValidConn() (*Connection, error) {
	// Sort hosts by workd=load
	pool.loadBalancer.SortHosts()
	// Search the host with lowest workload in idle queue
	for _, server := range pool.loadBalancer.ServerStatusList {
		for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
			if ele.Value.(*Connection).SeverAddress == *server.address {
				return ele.Value.(*Connection), nil
			}
		}
	}

	noAvaliableConnectionErr := errors.New("Failed to reconnect: no unused and valid connection can be found")
	return nil, noAvaliableConnectionErr
}
