/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

// TODO: add implement round-robin rule

package nebulaNet

import (
	"errors"
	"log"
	"sort"

	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type ServerStatus struct {
	address  *data.HostAddress
	isValid  bool
	workLoad int
}

type LoadBalancer struct {
	pool             *ConnectionPool
	ServerStatusList []*ServerStatus
}

func NewLoadbalancer(addresses []*data.HostAddress, pool *ConnectionPool) *LoadBalancer {
	newLoadBalancer := LoadBalancer{}
	newLoadBalancer.pool = pool
	for _, host := range addresses {
		newServerStatus := ServerStatus{}
		newServerStatus.address = host
		newServerStatus.isValid = false
		newServerStatus.workLoad = 0
		newLoadBalancer.ServerStatusList = append(newLoadBalancer.ServerStatusList, &newServerStatus)
	}
	return &newLoadBalancer
}

// Test if the host is valid
func PingHost(hostAddress *data.HostAddress) (bool, error) {
	conn := NewConnection(*hostAddress)
	conf := conf.NewPoolConf(0, 0, 0, 0)
	err := conn.Open(*hostAddress, conf)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return false, err
	}
	conn.Close()
	return true, nil
}

// Update all server's status (isValid)
func (loadBalancer *LoadBalancer) UpdateServerStatus() {
	for _, status := range loadBalancer.ServerStatusList {
		_, err := PingHost(status.address)
		if err != nil {
			log.Printf("Error: %s", err.Error())
			status.isValid = false
			status.address.IsAvaliable = false
			continue
		}
		status.isValid = true
	}
}

// Sort host by workload
func (loadBalancer *LoadBalancer) SortHosts() {
	sort.Slice(loadBalancer.ServerStatusList, func(i, j int) bool {
		return loadBalancer.ServerStatusList[i].workLoad < loadBalancer.ServerStatusList[j].workLoad
	})
}

// Returns the first host in the host list that is valid
// TODO: add tset for worklaod
func (loadBalancer *LoadBalancer) GetValidHost() (*data.HostAddress, error) {
	loadBalancer.SortHosts()
	for _, status := range loadBalancer.ServerStatusList {
		if status.isValid == true {
			return status.address, nil
		}
	}
	noAvaliableHost := errors.New("Failed to find an avaliable host in the connection pool")
	log.Print(noAvaliableHost)
	return nil, noAvaliableHost
}

// Mark the host as valid and increases its workload
func (loadBalancer *LoadBalancer) ValidateHost(SeverAddress *data.HostAddress) {
	for _, status := range loadBalancer.ServerStatusList {
		if status.address == SeverAddress {
			status.isValid = true
			status.workLoad = status.workLoad + 1
		}
	}
}
