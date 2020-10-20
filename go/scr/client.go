/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package ngdb

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

type GraphOptions struct {
	Timeout time.Duration
}

type GraphOption func(*GraphOptions)

var defaultGraphOptions = GraphOptions{
	Timeout: 0 * time.Second,
}

type GraphClient struct {
	graph     graph.GraphServiceClient
	option    GraphOptions
	sessionID int64
	timezone  int32
}

func WithTimeout(duration time.Duration) GraphOption {
	return func(options *GraphOptions) {
		options.Timeout = duration
	}
}

func NewClient(address string, opts ...GraphOption) (client *GraphClient, err error) {
	options := defaultGraphOptions
	for _, opt := range opts {
		opt(&options)
	}

	timeoutOption := thrift.SocketTimeout(options.Timeout)
	addressOption := thrift.SocketAddr(address)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return nil, err
	}

	transport := thrift.NewBufferedTransport(sock, 128<<10)

	pf := thrift.NewBinaryProtocolFactoryDefault()
	graph := &GraphClient{
		graph: *graph.NewGraphServiceClientFactory(transport, pf),
	}
	return graph, nil
}

// Open transport and authenticate
func (client *GraphClient) Connect(username, password string) error {
	if err := client.graph.Transport.Open(); err != nil {
		return err
	}

	resp, err := client.graph.Authenticate([]byte(username), []byte(password))
	if err != nil {
		log.Printf("Authentication fails, %s", err.Error())
		if e := client.graph.Close(); e != nil {
			log.Printf("Fail to close transport, error: %s", e.Error())
		}
		return err
	}

	if resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED {
		log.Printf("Authentication fails, ErrorCode: %v, ErrorMsg: %s", resp.GetErrorCode(), string(resp.GetErrorMsg()))
		return fmt.Errorf(string(resp.GetErrorMsg()))
	}

	client.sessionID = resp.GetSessionID()

	return nil
}

// Signout and close transport
func (client *GraphClient) Disconnect() {
	if err := client.graph.Signout(client.sessionID); err != nil {
		log.Printf("Fail to signout, error: %s", err.Error())
	}

	if err := client.graph.Close(); err != nil {
		log.Printf("Fail to close transport, error: %s", err.Error())
	}
}

func (client *GraphClient) Execute(stmt string) (*graph.ExecutionResponse, error) {
	return client.graph.Execute(client.sessionID, []byte(stmt))
}

// unsupported
// func (client *GraphClient) ExecuteJson(stmt string) (*graph.ExecutionResponse, error) {
// 	return client.graph.ExecuteJson(client.sessionID, []byte(stmt))
// }

func (client *GraphClient) GetSessionID() int64 {
	return client.sessionID
}

func IsError(resp *graph.ExecutionResponse) bool {
	return resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED
}

// detect if the given address and port is listening
func ping(address string, port int) bool {
	fmt.Fprintf(os.Stderr, "Testing local port %s:%d\n", address, port)
	portStr := strconv.Itoa(port)
	if len(portStr) == 0 {
		log.Printf("Invalid port: %s", portStr)
		return false
	}

	ln, err := net.Listen("tcp", ":"+portStr)
	if err != nil {
		log.Printf("Can't listen on port %s: %s \n", portStr, err)
		return false
	}

	err = ln.Close()
	if err != nil {
		log.Printf("Couldn't stop listening on port %s: %s \n", portStr, err)
		return false
	}

	fmt.Printf("TCP Port %s is available \n", portStr)
	return true
}

// func retryConnect() (client *GraphClient, err error) {

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
