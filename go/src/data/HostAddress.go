/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package data

type HostAddress struct {
	Host string
	Port int
	// valid bool
}

func NewHostAddress(Host string, Port int) HostAddress {
	NewHostAddress := HostAddress{}
	NewHostAddress.Host = Host
	NewHostAddress.Port = Port
	return NewHostAddress
}

func (hostAddress *HostAddress) SetHostAddress(Host string, Port int) {
	hostAddress.Host = Host
	hostAddress.Port = Port
}

func (hostAddress *HostAddress) GetHost() string {
	return hostAddress.Host
}

func (hostAddress *HostAddress) GetPort() int {
	return hostAddress.Port
}
