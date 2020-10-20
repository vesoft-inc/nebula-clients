/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package connectionPool

import (
	"fmt"
	"time"

	nebula "github.com/vesoft-inc/nebula-clients/go/nebula"
	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

// • init(addressses, configs)
// • addressses: graph服务的 ip 和端口集合，支持域名解析。
// • configs: 配置类
// • close(): 关闭所有的连接
// • return: void
// • getSession(userName, password, bool retryConnect = true): 获取 session 实例
// • param:
// • retryConnect: 用户是否需要让 session 自动重连
// • return: Session

type PoolConfigs struct {
	Idle_time                time.Duration
	Max_connection_pool_size int
	Min_connection_pool_size int
	Max_retry_times          int
}

type PoolConfig func(*PoolConfigs)

var defaultPoolConfigs = PoolConfigs{
	Idle_time:                0 * time.Second,
	Max_connection_pool_size: 100,
	Min_connection_pool_size: 0,
	Max_retry_times:          3,
}

type ConnectionPool struct {
	graph            graph.GraphServiceClient
	configs          PoolConfigs
	stop             bool
	clients          []*nebula.GraphClient
	idleClientsQueue chan *nebula.GraphClient
	reqCh            chan reqData
}

type Connection struct {
	Ip       string
	Port     int
	User     string
	Password string
}

func Set_PoolConfigs(duration time.Duration, Max_connection_pool_size int,
	Min_connection_pool_size int, Max_retry_times int) PoolConfig {
	return func(configs *PoolConfigs) {
		configs.Idle_time = duration
		configs.Max_connection_pool_size = Max_connection_pool_size
		configs.Min_connection_pool_size = Min_connection_pool_size
		configs.Max_retry_times = Max_retry_times
	}
}

//  graph服务的 ip 和端口集合，支持域名解析。
func initPool(addressses []string, conf PoolConfig) {
	configs := defaultPoolConfigs

}

type RespData struct {
	Resp *graph.ExecutionResponse
	Err  error
}

func close() {

}

func getSession(userName, password string, retryConnect bool) {

}

type SpaceDesc struct {
	Name          string
	Exists        bool
	NumPartitions int
	ReplicaFactor int
	Charset       string
	Collate       string
}

type Connection struct {
	Ip       string
	Port     int
	User     string
	Password string
}

type ConnectionPool interface {
	Close()
	Execute(stmt string) <-chan RespData
}

func (rsp RespData) IsError() bool {
	return rsp.Err != nil || nebula.IsError(rsp.Resp)
}

func (rsp RespData) String() string {
	if rsp.Err != nil {
		return fmt.Sprintf("Error: %s", rsp.Err.Error())
	}
	return rsp.Resp.String()
}

func (s SpaceDesc) CreateSpaceString() string {
	return fmt.Sprintf("CREATE SPACE IF NOT EXISTS `%s`(partition_num=%d, replica_factor=%d, charset=%s, collate=%s)",
		s.Name, s.NumPartitions, s.ReplicaFactor, s.Charset, s.Collate)
}

func (s SpaceDesc) UseSpaceString() string {
	return fmt.Sprintf("Use `%s`", s.Name)
}
