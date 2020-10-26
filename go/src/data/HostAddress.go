/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package data

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type HostAddress struct {
	Host        string
	Port        int
	IsAvaliable bool
}

func NewHostAddress(Host string, Port int) HostAddress {
	NewHostAddress := HostAddress{}
	NewHostAddress.Host = Host
	NewHostAddress.Port = Port
	NewHostAddress.IsAvaliable = true
	return NewHostAddress
}

func (hostAddress *HostAddress) SetHostAddress(Host string, Port int) {
	hostAddress.Host = Host
	hostAddress.Port = Port
	hostAddress.IsAvaliable = true
}

func (hostAddress *HostAddress) GetHost() string {
	return hostAddress.Host
}

func (hostAddress *HostAddress) GetPort() int {
	return hostAddress.Port
}

func IsIPv4(address string) bool {
	return strings.Count(address, ":") < 2
}

func DomainToIP(addresses []*HostAddress) error {
	for _, host := range addresses {
		// Get ip from domain
		ips, err := net.LookupIP(host.Host)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not get IPs: %v\n", err)
			return err
		}
		// Check if the address is IPV4
		for i, ip := range ips {
			if IsIPv4(ip.String()) {
				host.Host = ip.String()
				break
			}
			if i == len(ips)-1 {
				noIpv4 := errors.New("Invalid address: No IPV4 address found")
				log.Print(noIpv4)
				return noIpv4
			}
		}
	}
	return nil
}
