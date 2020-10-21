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

// type GraphOptions struct {
// 	Timeout time.Duration
// }

// func (opt *GraphOptions) SetDefualt() {
// 	opt.Timeout = 0 * time.Second
// }

// type GraphOption func(*GraphOptions)

// func WithTimeout(duration time.Duration) GraphOption {
// 	return func(options *GraphOptions) {
// 		options.Timeout = duration
// 	}
// }

type Session struct {
	// option     conf.GraphConfig
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

// func (session *Session) Open(hostaddress *data.HostAddress) error {
// 	ip := hostaddress.GetHost(hostaddress)
// 	port := hostaddress.GetPort(hostaddress)
// 	portStr := strconv.Itoa(port)
// 	newAddress := ip + ":" + portStr
// 	_, err := Open(newAddress)

// 	if err != nil {
// 		fmt.Println("Failed to create a new connection, ", err)
// 		return err
// 	}
// 	return nil
// }

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
		return false, err
	} else if resp != true {
		return false, err
	}
	return true, err
}

// func retryConnect() (session *Session, err error) {

// }

// func release() {

// }

// • 接口定义有以下
// • execute(string stmt): 执行ngql，返回的数据类型为 ResultSet
// • param:
// • stmt: 用户的ngql
// • return: ResultSet
// • executeJson(string stmt): 执行ngql，返回的数据类型为 Json 格式的字符串
// • param:
// • stmt: 用户的ngql
// • return: Json string
// • ping(): 用于测试和服务端的连通性
// • return: Bool
// • retryConnect(): 重连
// • return: ErrorCode or raise exception
// • release(): 做 signout，释放session id，归还connection到pool
// • return: void
