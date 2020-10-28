/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package main

import (
	"fmt"

	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	"github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
	nebulaNet "github.com/vesoft-inc/nebula-clients/go/src/net"
)

const (
	address  = "127.0.0.1"
	port     = 3699
	username = "user"
	password = "password"
)

func main() {
	hostAdress := data.NewHostAddress(address, port)
	hostList := []*data.HostAddress{}
	hostList = append(hostList, &hostAdress)
	pool := nebulaNet.ConnectionPool{}

	// Create configs for connection pool using default values
	testPoolConfig := conf.GetDefaultConf()
	// Initialize connectin pool
	err := pool.InitPool(hostList, &testPoolConfig)
	if err != nil {
		fmt.Printf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}
	// Create session
	session, err := pool.GetSession(username, password)
	if err != nil {
		fmt.Printf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			username, password, err.Error())
	}
	// Method used to check execution response
	checkResp := func(prefix string, err *graph.ExecutionResponse) {
		if nebulaNet.IsError(err) {
			fmt.Printf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
		}
	}
	// Excute a query
	resp, err := session.Execute("SHOW HOSTS;")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	checkResp("show hosts", resp)
	// Create a new space
	resp, err = session.Execute("CREATE SPACE client_test(partition_num=1024, replica_factor=1);")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	checkResp("create space", resp)

	resp, err = session.Execute("DROP SPACE client_test;")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	checkResp("drop space", resp)
	// Release session and return connection back to connection pool
	session.Release()
	// Close all connections in the pool
	pool.Close()

	fmt.Println("Example finished")
}
