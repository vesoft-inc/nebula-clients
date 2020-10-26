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
	port     = 3699
	username = "user"
	password = "password"
)

var poolAddress = []*data.HostAddress{
	&data.HostAddress{
		Host:        "127.0.0.1",
		Port:        3699,
		IsAvaliable: true,
	},
	&data.HostAddress{
		Host:        "127.0.0.1",
		Port:        3701,
		IsAvaliable: true,
	},
	&data.HostAddress{
		Host:        "127.0.0.1",
		Port:        3710,
		IsAvaliable: true,
	},
}

// Create default configs
var testPoolConfig = conf.NewPoolConf(0, 0, 0, 0, 0)

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

	conn := nebulaNet.NewConnection(hostAdress)
	err := conn.Open(hostAdress, testPoolConfig)
	if err != nil {
		t.Fatalf("Fail to open connection, address: %s, port: %d, %s", address, port, err.Error())
	}

	authresp, authErr := conn.Authenticate(username, password)
	if authErr != nil {
		t.Fatalf("Fail to authenticate, username: %s, password: %s, %s", username, password, authErr.Error())
	}

	sessionID := authresp.GetSessionID()

	defer logoutAndClose(conn, sessionID)

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

func TestPool_SingleHost(t *testing.T) {
	hostAdress := data.NewHostAddress(address, port)
	hostList := []*data.HostAddress{}
	hostList = append(hostList, &hostAdress)
	pool := nebulaNet.ConnectionPool{}

	// Initialize connectin pool
	err := pool.InitPool(hostList, &testPoolConfig)
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
			t.Errorf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
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

	err = session.Release()
	if err != nil {
		t.Fatalf("Fail to release session, %s", err.Error())
		return
	}

	err = pool.Close()
	if err != nil {
		t.Fatalf("Fail to close all connection in pool, %s", err.Error())
		return
	}
}

func TestPool_MultiHosts(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}

	// Try to get session while no idle connection avaliable
	pool.Close()

	_, err := pool.GetSession(username, password)
	if err != nil {
		t.Logf("Expected Failue: Fail to get session: no avaliable connection")
	}

	// Initialize connectin pool
	err = pool.InitPool(hostList, &testPoolConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s \n", address, port, err.Error())
	}

	var sessionList []*nebulaNet.Session

	// Take all idle connection and try to create a new session
	for i := 0; i < 3; i++ {
		session, err := pool.GetSession(username, password)
		if err != nil {
			t.Errorf("Fail to create a new session from connection pool, %s", err.Error())
		}
		sessionList = append(sessionList, session)
	}
	_, err = pool.GetSession(username, password)
	if err != nil {
		t.Logf("Expected Failue: No avaliable connection, %s", err.Error())
	}

	// Release 1 connectin back to pool
	sessionToRelease := sessionList[0]
	err = sessionToRelease.Release()
	sessionList = sessionList[1:]

	// Try again to get connection
	newSession, err := pool.GetSession(username, password)
	if err != nil {
		t.Logf("Expected Failue: No avaliable connection, %s", err.Error())
	}

	checkResp := func(prefix string, err *graph.ExecutionResponse) {
		if nebulaNet.IsError(err) {
			t.Fatalf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
		}
	}

	resp, err := newSession.Execute("SHOW HOSTS;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	checkResp("show hosts", resp)
}

func TestReconnect(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}
	timeoutConfig := conf.NewPoolConf(0, 0, 0, 6, 0)
	// Initialize connectin pool
	err := pool.InitPool(hostList, &timeoutConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}

	var sessionList []*nebulaNet.Session

	// Create session
	for i := 0; i < 3; i++ {
		session, err := pool.GetSession(username, password)
		if err != nil {
			t.Errorf("Fail to create a new session from connection pool, %s", err.Error())
		}
		sessionList = append(sessionList, session)
	}

	checkResp := func(prefix string, err *graph.ExecutionResponse) {
		if nebulaNet.IsError(err) {
			t.Errorf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
		}
	}

	for i := 0; i < 10; i++ {
		timer1 := time.NewTimer(1 * time.Second)
		<-timer1.C
		sessionList[0].Execute("SHOW HOSTS;")
		fmt.Println("Sending query...")
	}
	resp, err := sessionList[0].Execute("SHOW HOSTS;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	checkResp("show hosts", resp)

	err = sessionList[0].Release()
	if err != nil {
		t.Fatalf("Fail to release session, %s", err.Error())
		return
	}

	err = pool.Close()
	if err != nil {
		t.Fatalf("Fail to close all connection in pool, %s", err.Error())
		return
	}
}
