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
	SeverAddress data.HostAddress
	graph        *graph.GraphServiceClient
	isValid      bool
}

func NewConnection(severAddress data.HostAddress) *Connection {
	newObj := Connection{}
	newObj.SeverAddress = severAddress
	newObj.graph = nil
	return &newObj
}

func (cn *Connection) Open(hostAddress data.HostAddress, conf conf.PoolConfig) (err error) {
	ip := hostAddress.GetHost()
	port := hostAddress.GetPort()
	newAdd := ip + ":" + strconv.Itoa(port)
	timeoutOption := thrift.SocketTimeout(conf.TimeOut)
	addressOption := thrift.SocketAddr(newAdd)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		log.Printf("Failed to create a net.Conn-backed Transport,: %s", err.Error())
		return err
	}

	transport := thrift.NewBufferedTransport(sock, 128<<10)

	pf := thrift.NewBinaryProtocolFactoryDefault()
	cn.graph = graph.NewGraphServiceClientFactory(transport, pf)

	if err := cn.graph.Transport.Open(); err != nil {
		log.Printf("Failed to open transport, error: %s", err.Error())
		return err
	}
	if cn.graph.Transport.IsOpen() == false {
		log.Printf("Transport is off: \n")
	}
	return nil
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

// Sign out and release seesin ID
func (cn *Connection) SignOut(sessionID int64) error {
	// Release session ID to graphd
	if err := cn.graph.Signout(sessionID); err != nil {
		log.Printf("Fail to signout, error: %s", err.Error())
		return err
	}
	return nil
}

// Close transport
func (cn *Connection) Close() {
	cn.graph.Close()

}

func IsError(resp *graph.ExecutionResponse) bool {
	return resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED
}

func (cn *Connection) ValidateConnection() {
	cn.isValid = true
}

func (cn *Connection) InvalidateConnection() {
	cn.isValid = false
}

func (cn *Connection) GetisValid() bool {
	return cn.isValid
}

// Check connection to host address
func (cn *Connection) Ping() (bool, error) {
	_, err := cn.Execute(1, "YIELD 1")
	if err, ok := err.(thrift.TransportException); ok &&
		(err.TypeID() == thrift.END_OF_FILE || err.TypeID() == thrift.UNKNOWN_TRANSPORT_EXCEPTION) {
		cn.isValid = false
		return false, err
	}
	return true, nil
}
