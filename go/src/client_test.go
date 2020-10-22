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
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
	nebulaNet "github.com/vesoft-inc/nebula-clients/go/src/net"
)

const (
	address  = "127.0.0.1"
	port     = 34187
	username = "user"
	password = "password"
)

var DefaultPoolConfig = conf.PoolConfig{
	TimeOut:         1000 * time.Second,
	IdleTime:        0 * time.Second,
	MaxConnPoolSize: 100,
	MinConnPoolSize: 3,
	MaxRetryTimes:   3,
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

	conn := nebulaNet.NewConnection(hostAdress)
	err := conn.Open(hostAdress, DefaultPoolConfig)
	if err != nil {
		t.Fatalf("Fail to open connection, address: %s, port: %d, %s", address, port, err.Error())
	}

	authresp, authErr := conn.Authenticate(username, password)
	if authErr != nil {
		t.Fatalf("Fail to authenticate, username: %s, password: %s, %s", username, password, authErr.Error())
	}

	sessionID := authresp.GetSessionID()

	defer logoutAndClose(&conn, sessionID)

	checkResp := func(prefix string, authresp *graph.ExecutionResponse) {
		if nebulaNet.IsError(authresp) {
			t.Fatalf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, authresp.GetErrorCode(), authresp.GetErrorMsg())
		}
	}

	resp, err := conn.Execute(sessionID, "SHOW HOSTS;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}

	checkResp("show hosts", resp)

	resp, err = conn.Execute(sessionID, "CREATE SPACE client_test(partition_num=1024, replica_factor=1);")
	if err != nil {
		t.Error(err.Error())
		return
	}
	checkResp("create space", resp)
	resp, err = conn.Execute(sessionID, "DROP SPACE client_test;")
	if err != nil {
		t.Error(err.Error())
		return
	}
	checkResp("drop space", resp)

	_, err = conn.Ping(sessionID)
	if err != nil {
		t.Error(err.Error())
		return
	}

}

func TestPool(t *testing.T) {
	hostAdress := data.NewHostAddress(address, port)
	hostList := []*data.HostAddress{}
	hostList = append(hostList, &hostAdress)
	fmt.Sprintf("%s:%d", address, port)
	pool := nebulaNet.ConnectionPool{}

	err := pool.InitPool(hostList, &conf.DefaultPoolConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}

	session, err := pool.GetSession(username, password)
	if err != nil {
		t.Fatalf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			username, password, err.Error())
	}

	checkResp := func(prefix string, err *graph.ExecutionResponse) {
		if nebulaNet.IsError(err) {
			t.Fatalf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
		}
	}

	resp, err := session.Execute("SHOW HOSTS;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	checkResp("show hosts", resp)

	resp, err = session.Execute("CREATE SPACE client_test(partition_num=1024, replica_factor=1);")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	checkResp("create space", resp)

	resp, err = session.Execute("DROP SPACE client_test;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	checkResp("drop space", resp)
}
