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
	"sync"

	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type RespData struct {
	Resp *graph.ExecutionResponse
	Err  error
}

type ConnectionPool struct {
	idleConnectionQueue   list.List
	activeConnectionQueue list.List
	addresses             []data.HostAddress
	conf                  conf.PoolConfig
	isConnectionInUse     map[*Connection]bool
	rwLock                sync.RWMutex
	hostIndex             int
}

func (pool *ConnectionPool) InitPool(addresses []data.HostAddress, conf conf.PoolConfig) error {
	// Process domain to IP
	convAddress, err := data.DomainToIP(addresses)
	if err != nil {
		return fmt.Errorf("Failed to find IP, error: %s ", err.Error())
	}

	pool.addresses = convAddress
	pool.conf = conf
	pool.isConnectionInUse = make(map[*Connection]bool)
	pool.hostIndex = 0

	// Check input
	if len(addresses) == 0 {
		noAddresErr := errors.New("Failed to initialize connection pool: illegal address input")
		return noAddresErr
	}
	if &conf == nil {
		noConfErr := errors.New("Failed to initialize connection pool: no configuration")
		return noConfErr
	}

	for i := 0; i < pool.conf.MinConnPoolSize; i++ {
		// Simple round-robin
		newConn := NewConnection(pool.addresses[i%len(addresses)])
		// Not being used
		pool.isConnectionInUse[newConn] = false
		// Open connection to host
		err := newConn.Open(newConn.SeverAddress, pool.conf)
		if err != nil {
			return fmt.Errorf("Failed to open connection, error: %s ", err.Error())
		}
		// Mark connection as in use
		pool.isConnectionInUse[newConn] = true
		pool.idleConnectionQueue.PushBack(newConn)
	}
	return nil
}

func (pool *ConnectionPool) GetSession(username, password string) (*Session, error) {
	if &pool.conf == nil {
		notInitialized := errors.New("Failed to get session: There is no config in the connection pool")
		return nil, notInitialized
	}
	// Get valid and usable connection
	var conn *Connection = nil
	var err error = nil
	retryTimes := 3
	for i := 0; i < retryTimes; i++ {
		conn, err = pool.GetIdleConn()
		if err == nil {
			break
		}
	}
	if conn == nil {
		return nil, err
	}
	// Authenticate
	resp, err := conn.Authenticate(username, password)
	if err != nil || resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED {
		// if authentication failed, put connection back
		removeFromList(&pool.activeConnectionQueue, conn)
		pool.idleConnectionQueue.PushBack(conn)
		return nil, err
	}

	sessID := resp.GetSessionID()
	// Create new session
	newSession := Session{
		sessionID:  sessID,
		connection: conn,
		connPool:   pool,
	}

	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()
	// Mark connection as in use
	pool.isConnectionInUse[conn] = true

	return &newSession, nil
}

func (pool *ConnectionPool) GetIdleConn() (*Connection, error) {
	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()
	totalConn := pool.idleConnectionQueue.Len() + pool.activeConnectionQueue.Len()
	// Take an idle valid connection if possible
	if pool.idleConnectionQueue.Len() > 0 {
		var newConn *Connection = nil
		var newEle *list.Element = nil
		for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
			// Check if connection is valid
			if res := ele.Value.(*Connection).Ping(); res == true {
				newConn = ele.Value.(*Connection)
				newEle = ele
				break
			}
		}
		if newConn == nil {
			err := errors.New("Failed to get connection: No valid connection in the idle queue and connection number has reached the pool capacity")
			if totalConn < pool.conf.MaxConnPoolSize {
				newConn, err := pool.newConnToHost()
				if err != nil {
					return nil, err
				}
				// TODO: update workload
				return newConn, nil
			}
			return nil, err
		}
		// Remove new connection from idle and add to active if found
		pool.idleConnectionQueue.Remove(newEle)
		pool.activeConnectionQueue.PushBack(newConn)
		return newConn, nil
	}

	// Create a new connection if there is no idle connection and total connection < pool max size
	if totalConn < pool.conf.MaxConnPoolSize {
		newConn, err := pool.newConnToHost()
		if err != nil {
			return nil, err
		}
		// TODO: update workload
		return newConn, nil
	}
	// TODO: If no idle avaliable, wait for timeout and reconnect

	// If no idle avaliable and the number of total connection reaches the max pool size, return error/wait for timeout
	noAvaliableConnectionErr := errors.New("Failed to get connection: no avaliable connection in the connection pool")
	return nil, noAvaliableConnectionErr
}

// Release connection to pool
func (pool *ConnectionPool) RleaseObject(conn *Connection) {
	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()
	// Check if the connection is in use
	if pool.isConnectionInUse[conn] == true {
		pool.idleConnectionQueue.PushBack(conn)
		pool.isConnectionInUse[conn] = false

		removeFromList(&pool.activeConnectionQueue, conn)
	}
}

// Close all connection
func (pool *ConnectionPool) Close() {
	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()
	for conn := pool.idleConnectionQueue.Front(); conn != nil; conn = conn.Next() {
		conn.Value.(*Connection).Close()
		pool.idleConnectionQueue.Remove(conn)
	}
	for conn := pool.activeConnectionQueue.Front(); conn != nil; conn = conn.Next() {
		conn.Value.(*Connection).Close()
		pool.activeConnectionQueue.Remove(conn)
	}
}

func (pool *ConnectionPool) GetActiveConnCount() int {
	return pool.activeConnectionQueue.Len()
}

func (pool *ConnectionPool) GetIdleConnCount() int {
	return pool.idleConnectionQueue.Len()
}

// Get a valid host (round robin)
func (pool *ConnectionPool) getHost() data.HostAddress {
	if pool.hostIndex == len(pool.addresses) {
		pool.hostIndex = 0
	}
	host := pool.addresses[pool.hostIndex]
	pool.hostIndex++
	return host
}

// Select a new host to create a new connection
func (pool *ConnectionPool) newConnToHost() (*Connection, error) {
	// Get a valid host (round robin)
	host := pool.getHost()
	newConn := NewConnection(host)
	// Open connection to host
	err := newConn.Open(newConn.SeverAddress, pool.conf)
	if err != nil {
		return nil, err
	}
	// Add connection to active queue
	pool.activeConnectionQueue.PushBack(newConn)
	// TODO: update workload
	return newConn, nil
}

// Remove a connection from list
func removeFromList(l *list.List, conn *Connection) {
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		if *ele.Value.(*Connection) == *conn {
			l.Remove(ele)
		}
	}
}
