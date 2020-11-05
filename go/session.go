/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"fmt"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

type Session struct {
	sessionID  int64
	connection *connection
	connPool   *ConnectionPool
}

// unsupported
// func (session *Session) ExecuteJson(stmt string) (*graph.ExecutionResponse, error) {
// 	return session.graph.ExecuteJson(session.sessionID, []byte(stmt))
// }

// Execute a query
func (session *Session) Execute(stmt string) (*graph.ExecutionResponse, error) {
	if session.connection == nil {
		return nil, fmt.Errorf("Faied to execute: Session has been released")
	}
	resp, err := session.connection.execute(session.sessionID, stmt)
	if err != nil {
		// Reconnect only if the tranport is closed
		if err, ok := err.(thrift.TransportException); ok && err.TypeID() == thrift.END_OF_FILE {
			_err := session.reConnect()
			if _err != nil {
				session.connPool.log.Error(fmt.Sprintf("Failed to reconnect, %s \n", _err.Error()))
				return nil, _err
			}
			session.connPool.log.Info(fmt.Sprintf("Successfully reconnect to host: %s, port: %d \n",
				session.connection.SeverAddress.Host, session.connection.SeverAddress.Port))
			// Execute with the new connetion
			resp, err := session.connection.execute(session.sessionID, stmt)
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		// Reconnect fail
		session.connPool.log.Error(fmt.Sprintf("Error info: %s", err.Error()))
		return resp, err
	}
	return resp, nil
}

func (session *Session) reConnect() error {
	newconnection, err := session.connPool.getIdleConn()
	if err != nil {
		err = fmt.Errorf("Failed to reconnect: No idle connection, %s", err.Error())
		return err
	}
	// Close current connection
	session.connection.close()
	// Release connection to pool
	session.connPool.Rlease(session.connection)
	session.connection = newconnection
	return nil
}

// Logout and release connetion hold by session
func (session *Session) Release() {
	if session.connection == nil {
		session.connPool.log.Warn("Session has been released")
		return
	}
	if err := session.connection.signOut(session.sessionID); err != nil {
		session.connPool.log.Warn(fmt.Sprintf("Sign out failed, %s", err.Error()))
	}
	// Release connection to pool
	session.connPool.Rlease(session.connection)
	session.connection = nil
}
