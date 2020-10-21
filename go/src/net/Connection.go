/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"fmt"
	"log"
	"strconv"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type Connection struct {
	severAddress data.HostAddress
	graph        graph.GraphServiceClient
}

type ConnectionMethod interface {
	getServerAddress() data.HostAddress
	open(address data.HostAddress, timeout int)
	authenticate(username string, password string) int64
	close()
	ping()
	IsError(resp *graph.ExecutionResponse) bool
}

func newConnection(severAddress data.HostAddress, graph graph.GraphServiceClient) Connection {
	newObj := Connection{}
	newObj.severAddress = severAddress
	newObj.graph = graph

	return newObj
}

func (cn *Connection) GetServerAddress() data.HostAddress {
	return cn.severAddress
}

func Open(hostAddress data.HostAddress, opts conf.GraphConfig) (conn *Connection, err error) {
	defaultGraphOption := conf.GraphConfig{}
	defaultGraphOption.SetDefualt()
	options := defaultGraphOption
	// for _, opt := range opts {
	// 	opt(&options)
	// }
	ip := hostAddress.GetHost()
	port := hostAddress.GetPort()
	newAdd := ip + ":" + strconv.Itoa(port)
	timeoutOption := thrift.SocketTimeout(options.TimeOut)
	addressOption := thrift.SocketAddr(newAdd)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return nil, err
	}

	transport := thrift.NewBufferedTransport(sock, 128<<10)

	pf := thrift.NewBinaryProtocolFactoryDefault()
	con := &Connection{
		graph: *graph.NewGraphServiceClientFactory(transport, pf),
	}

	if err := con.graph.Transport.Open(); err != nil {
		return nil, err
	}

	return con, nil
}

// Open transport and authenticate
func (cn *Connection) Authenticate(username, password string) (*graph.AuthResponse, error) {
	resp, err := cn.graph.Authenticate([]byte(username), []byte(password))
	if err != nil {
		log.Printf("Authentication fails, %s", err.Error())
		if e := cn.graph.Close(); e != nil {
			log.Printf("Fail to close transport, error: %s", e.Error())
		}
		return nil, err
	}

	if resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED {
		log.Printf("Authentication fails, ErrorCode: %v, ErrorMsg: %s", resp.GetErrorCode(), string(resp.GetErrorMsg()))
		return nil, fmt.Errorf(string(resp.GetErrorMsg()))
	}

	// client.sessionID = resp.GetSessionID()

	return resp, nil
}

func (cn *Connection) Execute(sessionID int64, stmt string) (*graph.ExecutionResponse, error) {
	return cn.graph.Execute(sessionID, []byte(stmt))
}

// unsupported
// func (client *GraphClient) ExecuteJson((sessionID int64, stmt string) (*graph.ExecutionResponse, error) {
// 	return cn.graph.ExecuteJson(sessionID, []byte(stmt))
// }

// Signout
func (cn *Connection) SignOut(sessionID int64) {
	if err := cn.graph.Signout(sessionID); err != nil {
		log.Printf("Fail to signout, error: %s", err.Error())
	}
}

// Close transport
func (cn *Connection) Close() {
	if err := cn.graph.Close(); err != nil {
		log.Printf("Fail to close transport, error: %s", err.Error())
	}
}

func IsError(resp *graph.ExecutionResponse) bool {
	return resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED
}

// Check connection to host address
func (cn *Connection) Ping(sessionID int64) (bool, error) {
	resp, err := cn.Execute(sessionID, "YIELD 1")
	if err != nil {
		return false, err
	} else if IsError(resp) == true {
		return false, err
	}
	return true, err
}
