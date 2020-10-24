/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"fmt"

	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

type Session struct {
	sessionID  int64
	connection *Connection
	connPool   *ConnectionPool
}

func newSession(sessionID int64, connection *Connection,
	connPool *ConnectionPool) Session {
	newObj := Session{}
	newObj.sessionID = sessionID
	newObj.connection = connection
	newObj.connPool = connPool

	return newObj
}

// unsupported
// func (session *Session) ExecuteJson(stmt string) (*graph.ExecutionResponse, error) {
// 	return session.graph.ExecuteJson(session.sessionID, []byte(stmt))
// }

func (session *Session) Execute(stmt string) (*graph.ExecutionResponse, error) {
	resp, err := session.connection.Execute(session.sessionID, stmt)
	if err != nil {
		_err := session.reConnect()
		if _err != nil {
			fmt.Printf("Failed to reconnect, %s \n", _err.Error())
			return nil, _err
		}
		resp, err = session.connection.Execute(session.sessionID, stmt)
		if err != nil {
			fmt.Printf("Failed to execute, %s \n", err.Error())
			return resp, err
		}
		fmt.Printf("Successfully reconnect to host: %s, port: %d \n",
			session.connection.severAddress.GetHost(), session.connection.severAddress.GetPort())
		return resp, nil
	}
	return resp, err
}

// Check connection to host address
func (session *Session) Ping() (bool, error) {
	resp, err := session.connection.Ping(session.sessionID)
	if err != nil {
		fmt.Println("Failed to ping the server, ", err)
		return false, err
	} else if resp != true {
		fmt.Println("Failed to ping the server, ", err)
		return false, err
	}
	return true, err
}

func (session *Session) reConnect() error {
	newConnection, err := session.connPool.GetIdleConn()
	if err != nil {
		for retry := session.connPool.conf.MaxRetryTimes; retry != 0 && err != nil; retry-- {
			newConnection, err = session.connPool.GetIdleConn()
			if err == nil {
				goto next
			}
		}
		fmt.Println("Failed to reconnect: No idle connection, ", err)
		return err
	}

next:
	err = session.connection.Close()
	if err != nil {
		fmt.Println("Failed to reconnect: Cannot close current connection, ", err)
		return err
	}
	// Ready to use the new connection
	newConnection.inuse = true
	session.connPool.activeConnectionQueue.PushBack(newConnection)
	for e := session.connPool.idleConnectionQueue.Front(); e != nil; e = e.Next() {
		if e.Value == newConnection {
			session.connPool.idleConnectionQueue.Remove(e)
		}
	}

	session.connection = newConnection
	return nil
}

func (session *Session) Release() error {
	err := session.connection.SignOut(session.sessionID)
	if err != nil {
		fmt.Println("Failed to release session, ", err)
	}

	// Release connection to pool
	session.connPool.idleConnectionQueue.PushBack(session.connection)

	for ele := session.connPool.activeConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value == session.connection {
			session.connPool.activeConnectionQueue.Remove(ele)
		}
	}

	return nil
}
