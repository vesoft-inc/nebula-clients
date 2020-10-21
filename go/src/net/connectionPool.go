/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebulaNet

import (
	"fmt"

	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
)

var defaultPoolConfigs = conf.DefaultPoolConfig

type RespData struct {
	Resp *graph.ExecutionResponse
	Err  error
}

type ConnectionPool struct {
	session               *Session
	currentConnections    int
	idleConnectionQueue   []*Connection
	activeConnectionQueue []*Connection
	addressses            []string
	conf                  conf.PoolConfig
}

type ConnectionPoolMethod interface {
	initPool(addressses []string, conf conf.PoolConfig)
	getSession(username, password string)
	Execute(stmt string) <-chan RespData
	Close()
}

//  graph服务的 ip 和端口集合，支持域名解析。
func (pool *ConnectionPool) initPool(addressses []string, conf conf.PoolConfig) {
	pool.addressses = addressses
	pool.conf = conf

}

// func (rsp RespData) IsError() bool {
// 	return rsp.Err != nil || nebulaNet.IsError(rsp.Resp)
// }

// func (pool *ConnectionPool) getSession(username, password string) Session {
// 	if pool.idleClientsQueue > 0 {
// 		conn = pool.idleClientsQueue[0]
// 		hostAddress = conn.GetServerAddress()

// 		session := newConnection(graph graph.GraphServiceClient)
// 	}
// }

func (pool *ConnectionPool) Close() {
	for _, conn := range pool.activeConnectionQueue {
		conn.Close()
	}
}

func (rsp RespData) String() string {
	if rsp.Err != nil {
		return fmt.Sprintf("Error: %s", rsp.Err.Error())
	}
	return rsp.Resp.String()
}
