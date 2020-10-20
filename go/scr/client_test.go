/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package ngdb

import (
	"fmt"
	"testing"

	graph "github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

const (
	address  = "127.0.0.1"
	port     = 34187
	username = "user"
	password = "password"
)

// Before run `go test -v`, you should start a nebula server listening on 3699 port.
// Using docker-compose is the easiest way and you can reference this file:
//   https://github.com/vesoft-inc/nebula/blob/master/docker/docker-compose.yaml
//
// TODO(yee): introduce mock server
func TestClientConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping client test in short mode")
	}

	client, err := NewClient(fmt.Sprintf("%s:%d", address, port))

	if client.GetSessionID() != 0 {
		t.Errorf("Needed SessionID")
	}

	if err != nil {
		t.Errorf("Fail to create client, address: %s, port: %d, %s", address, port, err.Error())
	}

	if err = client.Connect(username, password); err != nil {
		t.Errorf("Fail to connect server, username: %s, password: %s, %s", username, password, err.Error())
	}

	defer client.Disconnect()

	checkResp := func(prefix string, resp *graph.ExecutionResponse) {
		if IsError(resp) {
			t.Logf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, resp.GetErrorCode(), resp.GetErrorMsg())
		}
	}

	resp, err := client.Execute("SHOW HOSTS;")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	checkResp("show hosts", resp)

	resp, err = client.Execute("CREATE SPACE client_test(partition_num=1024, replica_factor=1);")
	if err != nil {
		t.Error(err.Error())
		return
	}

	resp, err = client.Execute("DROP SPACE client_test;")
	if err != nil {
		t.Error(err.Error())
		return
	}

	checkResp("create space", resp)

}

func TestClientPing(t *testing.T) {
	if ping(address, port) == true {

	}

}
