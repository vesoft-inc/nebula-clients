/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"fmt"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
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
		// Reconnect only if the tranport is closed
		if err, ok := err.(thrift.TransportException); ok && err.TypeID() == thrift.END_OF_FILE {
			_err := session.reConnect()
			if _err != nil {
				fmt.Printf("Failed to reconnect, %s \n", _err.Error())
				return nil, _err
			}

			fmt.Printf("Successfully reconnect to host: %s, port: %d \n",
				session.connection.SeverAddress.GetHost(), session.connection.SeverAddress.GetPort())
			// Execute with the new connetion
			resp, err := session.connection.Execute(session.sessionID, stmt)
			if err != nil {
				fmt.Sprintf("Error info: %s", err.Error())
			}
			return resp, nil
		}

		fmt.Sprintf("Error info: %s", err.Error())
		return resp, err
	}
	return resp, nil
}

// Check connection to host address
func (session *Session) Ping() (bool, error) {
	_, err := session.connection.Ping()
	if err != nil {
		fmt.Println("Failed to ping the server, ", err)
		return false, err
	}
	return true, err
}

func (session *Session) reConnect() error {
	newConnection, err := session.connPool.GetIdleConn()
	if err != nil {
		newConnection, err = session.connPool.GetIdleConn()
		if err == nil {
			goto next
		}
		fmt.Println("Failed to reconnect: No idle connection, ", err)
		return err
	}

next:
	// Close current connection
	session.connection.Close()

	session.connection = newConnection
	return nil
}

func (session *Session) Release() {
	session.connection.SignOut(session.sessionID)
	// Release connection to pool
	session.connPool.ReturnObject(session.connection)
}
