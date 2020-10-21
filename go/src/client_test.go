/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package ngdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	"github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
	nebulaNet "github.com/vesoft-inc/nebula-clients/go/src/net"
)

const (
	address  = "127.0.0.1"
	port     = 34187
	username = "user"
	password = "password"
)

var graphConfig = conf.GraphConfig{
	TimeOut: 0 * time.Second,
}

// Before run `go test -v`, you should start a nebula server listening on 3699 port.
// Using docker-compose is the easiest way and you can reference this file:
//   https://github.com/vesoft-inc/nebula/blob/master/docker/docker-compose.yaml
//
// TODO(yee): introduce mock server

func logoutAndClose(conn *nebulaNet.Connection, sessionID int64) {
	conn.SignOut(sessionID)
	conn.Close()
}
func TestConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping client test in short mode")
	}

	hostAdress := data.NewHostAddress(address, port)
	fmt.Sprintf("%s:%d", address, port)
	conn, err := nebulaNet.Open(hostAdress, graphConfig)
	if err != nil {
		t.Errorf("Fail to open connection, address: %s, port: %d, %s", address, port, err.Error())
	}

	authresp, err := conn.Authenticate(username, password)
	if err != nil {
		t.Errorf("Fail to authenticate, username: %s, password: %s, %s", username, password, err.Error())
	}

	sessionID := authresp.GetSessionID()

	defer logoutAndClose(conn, sessionID)

	checkResp := func(prefix string, authresp *graph.ExecutionResponse) {
		if nebulaNet.IsError(authresp) {
			t.Logf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, authresp.GetErrorCode(), authresp.GetErrorMsg())
		}
	}

	resp, err := conn.Execute(sessionID, "SHOW HOSTS;")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	checkResp("show hosts", resp)

	resp, err = conn.Execute(sessionID, "CREATE SPACE client_test(partition_num=1024, replica_factor=1);")
	if err != nil {
		t.Error(err.Error())
		return
	}

	resp, err = conn.Execute(sessionID, "DROP SPACE client_test;")
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = conn.Ping(sessionID)
	if err != nil {
		t.Error(err.Error())
		return
	}

	checkResp("create space", resp)

}

// func TestClientPing(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping client test in short mode")
// 	}

// 	client, err := nebulaNet.Open(fmt.Sprintf("%s:%d", address, port))
// 	if client.GetSessionID() != 0 {
// 		t.Errorf("Needed SessionID")
// 	}

// 	if err != nil {
// 		t.Errorf("Fail to create client, address: %s, port: %d, %s", address, port, err.Error())
// 	}

// 	if err = client.Connect(username, password); err != nil {
// 		t.Errorf("Fail to connect server, username: %s, password: %s, %s", username, password, err.Error())
// 	}

// 	defer client.Release()

// 	result, err := client.Ping(address, port)
// 	if err != nil {
// 		t.Errorf("Connection lost, address: %s, port: %d, %s", address, port, err.Error())
// 	}
// 	if result == true {
// 		t.Logf("Ping to server succeed, address: %s, port: %d, %s", address, port, err.Error())
// 	}
// 	return
// }
