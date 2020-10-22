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
	connection Connection
	connPool   ConnectionPool
}

func newSession(sessionID int64, connection Connection,
	connPool ConnectionPool) Session {
	newObj := Session{}
	newObj.sessionID = sessionID
	newObj.connection = connection
	newObj.connPool = connPool

	return newObj
}

func (session *Session) Authenticate(username string, password string) (int64, error) {
	resp, err := session.connection.Authenticate(username, password)
	if err != nil {
		fmt.Println("Failed to authenticate, ", err)
		return 0, err
	}
	session.sessionID = resp.GetSessionID()
	return session.sessionID, nil
}

func (session *Session) GetSessionID() int64 {
	return session.sessionID
}

// unsupported
// func (session *Session) ExecuteJson(stmt string) (*graph.ExecutionResponse, error) {
// 	return session.graph.ExecuteJson(session.sessionID, []byte(stmt))
// }

func (session *Session) Execute(stmt string) (*graph.ExecutionResponse, error) {
	resp, err := session.connection.Execute(session.sessionID, stmt)
	if err != nil {
		fmt.Println("Failed to execuute, ", err)
		return resp, err
	}
	return resp, nil
}

// Check connection to host address
func (session *Session) Ping() (bool, error) {
	resp, err := session.connection.Ping(session.GetSessionID())
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
	newConnection, err := session.connPool.GetIdleConn(session.connection.severAddress)
	if err != nil {
		fmt.Println("Failed to get am odle connection, ", err)
		return err
	}

	err = session.connection.Close()
	if err != nil {
		fmt.Println("Failed to close current connection, ", err)
		return err
	}
	session.connection = *newConnection
	return nil
}

func (session *Session) release() error {
	err := session.connection.SignOut(session.GetSessionID())
	if err != nil {
		fmt.Println("Failed to release session, ", err)
	}
	return nil
}
