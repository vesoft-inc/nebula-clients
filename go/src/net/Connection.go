/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"fmt"
	"log"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type Connection struct {
	SeverAddress data.HostAddress
	graph        *graph.GraphServiceClient
	IsValid      bool
}

func NewConnection(severAddress data.HostAddress) *Connection {
	return &Connection{
		SeverAddress: severAddress,
		graph:        nil,
	}
}

func (cn *Connection) Open(hostAddress data.HostAddress, conf conf.PoolConfig) (err error) {
	ip := hostAddress.Host
	port := hostAddress.Port
	newAdd := fmt.Sprintf("%s:%d", ip, port)
	timeoutOption := thrift.SocketTimeout(conf.TimeOut)
	addressOption := thrift.SocketAddr(newAdd)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		err = fmt.Errorf("Failed to create a net.Conn-backed Transport,: %s", err.Error())
		return
	}

	transport := thrift.NewBufferedTransport(sock, 128<<10)
	pf := thrift.NewBinaryProtocolFactoryDefault()
	cn.graph = graph.NewGraphServiceClientFactory(transport, pf)

	if err = cn.graph.Transport.Open(); err != nil {
		err = fmt.Errorf("Failed to open transport, error: %s", err.Error())
		return
	}
	if cn.graph.Transport.IsOpen() == false {
		log.Printf("Transport is off: \n")
	}
	return nil
}

// Authenticate
func (cn *Connection) Authenticate(username, password string) (*graph.AuthResponse, error) {
	resp, err := cn.graph.Authenticate([]byte(username), []byte(password))
	if err != nil {
		err = fmt.Errorf("Authentication fails, %s", err.Error())
		if e := cn.graph.Close(); e != nil {
			err = fmt.Errorf("Fail to close transport, error: %s", e.Error())
		}
		return nil, err
	}
	return resp, err
}

func (cn *Connection) Execute(sessionID int64, stmt string) (*graph.ExecutionResponse, error) {
	return cn.graph.Execute(sessionID, []byte(stmt))
}

// unsupported
// func (client *GraphClient) ExecuteJson((sessionID int64, stmt string) (*graph.ExecutionResponse, error) {
// 	return cn.graph.ExecuteJson(sessionID, []byte(stmt))
// }

// Check connection to host address
func (cn *Connection) Ping() bool {
	_, err := cn.Execute(1, "YIELD 1")
	if err != nil {
		cn.IsValid = false
		return false
	}
	return true
}

// Sign out and release seesin ID
func (cn *Connection) SignOut(sessionID int64) {
	// Release session ID to graphd
	if err := cn.graph.Signout(sessionID); err != nil {
		log.Printf("Fail to signout \n")
	}
}

// Close transport
func (cn *Connection) Close() {
	cn.graph.Close()
}

func IsError(resp *graph.ExecutionResponse) bool {
	return resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED
}
