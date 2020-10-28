/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package ngdb

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
var testPoolConfig = conf.GetDefaultConf()

// Before run `go test -v`, you should start a nebula server listening on 3699 port.
// Using docker-compose is the easiest way and you can reference this file:
//   https://github.com/vesoft-inc/nebula/blob/master/docker/docker-compose.yaml

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

	checkResp := func(prefix string, execResp *graph.ExecutionResponse) {
		if nebulaNet.IsError(execResp) {
			t.Fatalf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, execResp.GetErrorCode(), execResp.GetErrorMsg())
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

	_, err = conn.Ping()
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

	testPoolConfig = conf.NewPoolConf(0, 0, 10, 1)
	// Initialize connectin pool
	err := pool.InitPool(hostList, &testPoolConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}
	// Create session
	session, err := pool.GetSession(username, password)
	if err != nil {
		t.Fatalf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			username, password, err.Error())
	}
	// Method used to check execution response
	checkResp := func(prefix string, err *graph.ExecutionResponse) {
		if nebulaNet.IsError(err) {
			t.Errorf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
		}
	}
	// Excute a query
	resp, err := session.Execute("SHOW HOSTS;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	checkResp("show hosts", resp)
	// Create a new space
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
	// Release session and return connection back to connection pool
	session.Release()

	// Close all connections in the pool
	pool.Close()
}

func TestPool_MultiHosts(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}

	// Try to get session while no idle connection avaliable
	pool.Close()

	_, err := pool.GetSession(username, password)
	if assert.Equal(t, err.Error(), "Failed to get session: There is no config in the connection pool") {
		t.Logf("Expected error: Failed to get session: There is no config in the connection pool")
	}

	// Minimun pool size < hosts number
	multiHostsConfig := conf.NewPoolConf(0, 0, 3, 1)
	// Initialize connectin pool
	err = pool.InitPool(hostList, &multiHostsConfig)
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
	sessionToRelease.Release()
	sessionList = sessionList[1:]

	// Try again to get connection
	newSession, err := pool.GetSession(username, password)
	if err != nil {
		t.Logf("Fail to create a new session, %s", err.Error())
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

func TestMultiThreads(t *testing.T) {
	hostAdress := data.NewHostAddress(address, port)
	hostList := []*data.HostAddress{}
	hostList = append(hostList, &hostAdress)
	pool := nebulaNet.ConnectionPool{}

	testPoolConfig = conf.NewPoolConf(0, 0, 1000, 1)
	// Initialize connectin pool
	err := pool.InitPool(hostList, &testPoolConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}
	var sessionList []*nebulaNet.Session

	var w sync.WaitGroup
	var mu sync.RWMutex
	// Create multiple session
	for i := 0; i < 1000; i++ {
		w.Add(1)
		go func(wg *sync.WaitGroup) {
			session, err := pool.GetSession(username, password)
			if err != nil {
				t.Errorf("Fail to create a new session from connection pool, %s", err.Error())
			}
			mu.Lock()
			sessionList = append(sessionList, session)
			mu.Unlock()
			wg.Done()
		}(&w)
	}
	w.Wait()
	if assert.Equal(t, pool.GetActiveConnCount(), 1000) {
		t.Logf("Expected total active connections: 1000")
	}
	for i := 0; i < 1000; i++ {
		sessionList[i].Release()
	}
	if assert.Equal(t, pool.GetIdleConnCount(), 1000) {
		t.Logf("Expected total idle connections: 1000")
	}
}

func TestReconnect(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}
	timeoutConfig := conf.NewPoolConf(0, 0, 10, 6)
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

	// Send query to server periodically
	for i := 0; i < 10; i++ {
		timer1 := time.NewTimer(1 * time.Second)
		<-timer1.C
		_, err := sessionList[0].Execute("SHOW HOSTS;")
		fmt.Println("Sending query...")

		if err != nil {
			t.Errorf("Error info: %s", err.Error())
			return
		}
	}

	resp, err := sessionList[0].Execute("SHOW HOSTS;")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}

	// This assertion will pass only when reconnection happens
	if assert.Equal(t, resp.GetErrorCode(), graph.ErrorCode_E_SESSION_INVALID) {
		t.Logf("Expected error: E_SESSION_INVALID")
	}

	sessionList[0].Release()
	if err != nil {
		t.Fatalf("Fail to release session, %s", err.Error())
		return
	}
	pool.Close()
}

func TestIpLookup(t *testing.T) {
	ips, err := net.LookupIP("google.com")
	if err != nil {
		t.Errorf("Could not get IPs: %v\n", err)
	}
	for _, ip := range ips {
		fmt.Printf("google.com. IN A %s\n", ip.String())
	}
}

func TestIPV4Validation(t *testing.T) {
	result := data.IsIPv4("192.168.0.1")
	assert.Equal(t, result, true, "192.168.0.1 is an IPV4 address")
	result = data.IsIPv4("::FFFF:C0A8:1")
	assert.Equal(t, result, false, "::FFFF:C0A8:1 is not an IPV4 address")
}
