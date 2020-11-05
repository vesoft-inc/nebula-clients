/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"container/list"
	"fmt"
	"sync"

	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

type ConnectionPool struct {
	idleconnectionQueue   list.List
	activeconnectionQueue list.List
	addresses             []HostAddress
	conf                  PoolConfig
	hostIndex             int
	log                   DefaultLogger
	rwLock                sync.RWMutex
}

func (pool *ConnectionPool) InitPool(addresses []HostAddress, conf PoolConfig, log DefaultLogger) error {
	// Process domain to IP
	convAddress, err := DomainToIP(addresses)
	if err != nil {
		return fmt.Errorf("Failed to find IP, error: %s ", err.Error())
	}

	pool.addresses = convAddress
	pool.conf = conf
	pool.hostIndex = 0
	pool.log = log
	// Check input
	if len(addresses) == 0 {
		return fmt.Errorf("Failed to initialize connection pool: illegal address input")
	}
	if &conf == nil {
		return fmt.Errorf("Failed to initialize connection pool: no configuration")
	}

	for i := 0; i < pool.conf.MinConnPoolSize; i++ {
		// Simple round-robin
		newConn := newConnection(pool.addresses[i%len(addresses)])

		// Open connection to host
		err := newConn.open(newConn.SeverAddress, pool.conf)
		if err != nil {
			return fmt.Errorf("Failed to open connection, error: %s ", err.Error())
		}
		// Mark connection as in use
		pool.idleconnectionQueue.PushBack(newConn)
	}
	pool.log.Info("connection pool is initialized successfully")
	return nil
}

func (pool *ConnectionPool) GetSession(username, password string) (*Session, error) {
	if &pool.conf == nil {
		return nil, fmt.Errorf("Failed to get session: There is no config in the connection pool")
	}
	// Get valid and usable connection
	var conn *connection = nil
	var err error = nil
	retryTimes := 3
	for i := 0; i < retryTimes; i++ {
		conn, err = pool.getIdleConn()
		if err == nil {
			break
		}
	}
	if conn == nil {
		return nil, err
	}
	// Authenticate
	resp, err := conn.authenticate(username, password)
	if err != nil || resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED {
		// if authentication failed, put connection back
		pool.rwLock.Lock()
		defer pool.rwLock.Unlock()
		removeFromList(&pool.activeconnectionQueue, conn)
		pool.idleconnectionQueue.PushBack(conn)
		return nil, err
	}

	sessID := resp.GetSessionID()
	// Create new session
	newSession := Session{
		sessionID:  sessID,
		connection: conn,
		connPool:   pool,
	}

	return &newSession, nil
}

func (pool *ConnectionPool) getIdleConn() (*connection, error) {
	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()

	// Take an idle valid connection if possible
	if pool.idleconnectionQueue.Len() > 0 {
		var newConn *connection = nil
		var newEle *list.Element = nil
		for ele := pool.idleconnectionQueue.Front(); ele != nil; ele = ele.Next() {
			// Check if connection is valid
			if res := ele.Value.(*connection).ping(); res == true {
				newConn = ele.Value.(*connection)
				newEle = ele
				break
			}
		}
		if newConn == nil {
			newConn, err := pool.createconnection()
			return newConn, err
		}
		// Remove new connection from idle and add to active if found
		pool.idleconnectionQueue.Remove(newEle)
		pool.activeconnectionQueue.PushBack(newConn)
		return newConn, nil
	}

	// Create a new connection if there is no idle connection and total connection < pool max size
	newConn, err := pool.createconnection()
	return newConn, err
	// TODO: If no idle avaliable, wait for timeout and reconnect
}

// Release connection to pool
func (pool *ConnectionPool) Rlease(conn *connection) {
	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()
	// Remove connection from active queue and add into idle queue
	removeFromList(&pool.activeconnectionQueue, conn)
	pool.idleconnectionQueue.PushBack(conn)
}

// Close all connection
func (pool *ConnectionPool) Close() {
	pool.rwLock.Lock()
	defer pool.rwLock.Unlock()
	for conn := pool.idleconnectionQueue.Front(); conn != nil; conn = conn.Next() {
		conn.Value.(*connection).close()
		pool.idleconnectionQueue.Remove(conn)
	}
	for conn := pool.activeconnectionQueue.Front(); conn != nil; conn = conn.Next() {
		conn.Value.(*connection).close()
		pool.activeconnectionQueue.Remove(conn)
	}
}

func (pool *ConnectionPool) getActiveConnCount() int {
	return pool.activeconnectionQueue.Len()
}

func (pool *ConnectionPool) getIdleConnCount() int {
	return pool.idleconnectionQueue.Len()
}

// Get a valid host (round robin)
func (pool *ConnectionPool) getHost() HostAddress {
	if pool.hostIndex == len(pool.addresses) {
		pool.hostIndex = 0
	}
	host := pool.addresses[pool.hostIndex]
	pool.hostIndex++
	return host
}

// Select a new host to create a new connection
func (pool *ConnectionPool) newConnToHost() (*connection, error) {
	// Get a valid host (round robin)
	host := pool.getHost()
	newConn := newConnection(host)
	// Open connection to host
	err := newConn.open(newConn.SeverAddress, pool.conf)
	if err != nil {
		return nil, err
	}
	// Add connection to active queue
	pool.activeconnectionQueue.PushBack(newConn)
	// TODO: update workload
	return newConn, nil
}

// Remove a connection from list
func removeFromList(l *list.List, conn *connection) {
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		if *ele.Value.(*connection) == *conn {
			l.Remove(ele)
		}
	}
}

// Compare total connection number with pool max size and return a connection if capable
func (pool *ConnectionPool) createconnection() (*connection, error) {
	totalConn := pool.idleconnectionQueue.Len() + pool.activeconnectionQueue.Len()
	// If no idle avaliable and the number of total connection reaches the max pool size, return error/wait for timeout
	if totalConn >= pool.conf.MaxConnPoolSize {
		return nil, fmt.Errorf("Failed to get connection: No valid connection in the idle queue and connection number has reached the pool capacity")
	}

	newConn, err := pool.newConnToHost()
	if err != nil {
		return nil, err
	}
	// TODO: update workload
	return newConn, nil
}
