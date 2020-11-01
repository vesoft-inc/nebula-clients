/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package ngdb

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
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

var poolAddress = []data.HostAddress{
	data.HostAddress{
		Host: "127.0.0.1",
		Port: 3699,
	},
	data.HostAddress{
		Host: "127.0.0.1",
		Port: 3701,
	},
	data.HostAddress{
		Host: "127.0.0.1",
		Port: 3710,
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

	hostAdress := data.HostAddress{Host: address, Port: port}

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

	res := conn.Ping()
	if res != true {
		t.Error("Connectin ping failed")
		return
	}

}

func TestPool_SingleHost(t *testing.T) {
	hostAdress := data.HostAddress{Host: address, Port: port}
	hostList := []data.HostAddress{}
	hostList = append(hostList, hostAdress)
	pool := nebulaNet.ConnectionPool{}

	var (
		TimeOut         = 0 * time.Millisecond
		IdleTime        = 0 * time.Millisecond
		MaxConnPoolSize = 10
		MinConnPoolSize = 1
	)
	testPoolConfig = conf.NewPoolConf(TimeOut, IdleTime, MaxConnPoolSize, MinConnPoolSize)
	// Initialize connectin pool
	err := pool.InitPool(hostList, testPoolConfig)
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
	defer func() {
		// Release session and return connection back to connection pool
		session.Release()
		// Close all connections in the pool
		pool.Close()
	}()
}

func TestPool_MultiHosts(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}

	// Try to get session while no idle connection avaliable
	_, err := pool.GetSession(username, password)
	assert.Equal(t, "Failed to get connection: no avaliable connection in the connection pool", err.Error())

	// Minimun pool size < hosts number
	var (
		TimeOut         = 0 * time.Millisecond
		IdleTime        = 0 * time.Millisecond
		MaxConnPoolSize = 3
		MinConnPoolSize = 1
	)
	multiHostsConfig := conf.NewPoolConf(TimeOut, IdleTime, MaxConnPoolSize, MinConnPoolSize)

	// Initialize connectin pool
	err = pool.InitPool(hostList, multiHostsConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s \n", address, port, err.Error())
	}

	var sessionList []*nebulaNet.Session

	// Take all idle connection and try to create a new session
	for i := 0; i < MaxConnPoolSize; i++ {
		session, err := pool.GetSession(username, password)
		if err != nil {
			t.Errorf("Fail to create a new session from connection pool, %s", err.Error())
		}
		sessionList = append(sessionList, session)
	}
	_, err = pool.GetSession(username, password)
	assert.Equal(t, "Failed to get connection: no avaliable connection in the connection pool", err.Error())

	// Release 1 connectin back to pool
	sessionToRelease := sessionList[0]
	sessionToRelease.Release()
	sessionList = sessionList[1:]

	// Try again to get connection
	newSession, err := pool.GetSession(username, password)
	if err != nil {
		t.Errorf("Fail to create a new session, %s", err.Error())
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

	// Try to get more session when the pool is full
	newSession, err = pool.GetSession(username, password)
	assert.Equal(t, "Failed to get connection: no avaliable connection in the connection pool", err.Error())

	defer func() {
		for i := 0; i < len(sessionList); i++ {
			sessionList[i].Release()
		}
		pool.Close()
	}()
}

func TestMultiThreads(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}

	var (
		TimeOut         = 0 * time.Millisecond
		IdleTime        = 0 * time.Millisecond
		MaxConnPoolSize = 666
		MinConnPoolSize = 1
	)
	testPoolConfig := conf.NewPoolConf(TimeOut, IdleTime, MaxConnPoolSize, MinConnPoolSize)

	// Initialize connectin pool
	err := pool.InitPool(hostList, testPoolConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}
	var sessionList []*nebulaNet.Session

	// Create multiple session
	var wg sync.WaitGroup
	sessCh := make(chan *nebulaNet.Session)
	done := make(chan bool)
	wg.Add(MaxConnPoolSize)
	for i := 0; i < MaxConnPoolSize; i++ {
		go func(sessCh chan *nebulaNet.Session, wg *sync.WaitGroup) {
			defer wg.Done()
			session, err := pool.GetSession(username, password)
			if err != nil {
				t.Errorf("Fail to create a new session from connection pool, %s", err.Error())
			}
			sessCh <- session
		}(sessCh, &wg)

	}
	go func(sessCh chan *nebulaNet.Session) {
		for session := range sessCh {
			sessionList = append(sessionList, session)
		}
		done <- true
	}(sessCh)
	wg.Wait()
	close(sessCh)
	<-done

	if assert.Equal(t, 666, pool.GetActiveConnCount()) {
		t.Logf("Expected total active connections: 666, Actual value: %d", pool.GetActiveConnCount())
	}
	if assert.Equal(t, 666, len(sessionList)) {
		t.Logf("Expected total sessions: 666, Actual value: %d", len(sessionList))
	}
	// for i := 0; i < len(hostList); i++ {
	// 	assert.Equal(t, 222, pool.GetServerWorkload(i))
	// }
	for i := 0; i < MaxConnPoolSize; i++ {
		sessionList[i].Release()
	}
	if assert.Equal(t, 666, pool.GetIdleConnCount()) {
		t.Logf("Expected total idle connections: 666, Actual value: %d", pool.GetIdleConnCount())
	}
	defer func() {
		for i := 0; i < len(sessionList); i++ {
			sessionList[i].Release()
		}
		pool.Close()
	}()
}

func TestLoadbalancer(t *testing.T) {
	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}

	var (
		TimeOut         = 0 * time.Millisecond
		IdleTime        = 0 * time.Millisecond
		MaxConnPoolSize = 999
		MinConnPoolSize = 1
	)
	testPoolConfig := conf.NewPoolConf(TimeOut, IdleTime, MaxConnPoolSize, MinConnPoolSize)

	// Initialize connectin pool
	err := pool.InitPool(hostList, testPoolConfig)
	if err != nil {
		t.Fatalf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}
	var sessionList []*nebulaNet.Session

	// Create multiple sessions
	for i := 0; i < 999; i++ {
		session, err := pool.GetSession(username, password)
		if err != nil {
			t.Errorf("Fail to create a new session from connection pool, %s", err.Error())
		}
		sessionList = append(sessionList, session)
	}
	if assert.Equal(t, len(sessionList), 999) {
		t.Logf("Expected total sessions: 999, Actual value: %d", len(sessionList))
	}
	// for i := 0; i < len(hostList); i++ {
	// 	assert.Equal(t, pool.GetServerWorkload(i), 333)
	// }

	defer func() {
		for i := 0; i < len(sessionList); i++ {
			sessionList[i].Release()
		}
		pool.Close()
	}()
}

func TestReconnect(t *testing.T) {
	// Set up docker client
	client, err := client.NewEnvClient()
	if err != nil {
		fmt.Printf("Unable to create docker client: %s", err)
	}

	hostList := poolAddress
	pool := nebulaNet.ConnectionPool{}

	var (
		TimeOut         = 0 * time.Millisecond
		IdleTime        = 0 * time.Millisecond
		MaxConnPoolSize = 10
		MinConnPoolSize = 6
	)
	timeoutConfig := conf.NewPoolConf(TimeOut, IdleTime, MaxConnPoolSize, MinConnPoolSize)

	// Initialize connectin pool
	err = pool.InitPool(hostList, timeoutConfig)
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
	for i := 0; i < MaxConnPoolSize; i++ {
		time.Sleep(1 * time.Second)
		if i == 3 {
			stopContainer(client, "nebula-docker-compose_graphd_1")
		}
		if i == 7 {
			stopContainer(client, "nebula-docker-compose_graphd2_1")
		}
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

	startContainer(client, "nebula-docker-compose_graphd_1")
	startContainer(client, "nebula-docker-compose_graphd2_1")
	sessionList[0].Release()
	if err != nil {
		t.Fatalf("Fail to release session, %s", err.Error())
		return
	}
	defer func() {
		for i := 0; i < len(sessionList); i++ {
			sessionList[i].Release()
		}
		pool.Close()
	}()
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

func stopContainer(client *client.Client, containername string) error {
	ctx := context.Background()

	if err := client.ContainerStop(ctx, containername, nil); err != nil {
		log.Panicf("Unable to stop container %s: %s", containername, err)
	}
	return nil
}

func startContainer(client *client.Client, containername string) error {
	ctx := context.Background()

	if err := client.ContainerStart(ctx, containername, types.ContainerStartOptions{}); err != nil {
		log.Panicf("Unable to start container %s: %s", containername, err)
	}
	return nil
}
