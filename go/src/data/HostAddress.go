/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package data

type HostAddress struct {
	host string
	port int
}

func NewHostAddress(host string, port int) HostAddress {
	NewHostAddress := HostAddress{}
	NewHostAddress.host = host
	NewHostAddress.port = port
	return NewHostAddress
}

func (hostAddress *HostAddress) SetHostAddress(host string, port int) {
	hostAddress.host = host
	hostAddress.port = port
}

func (hostAddress *HostAddress) GetHost() string {
	return hostAddress.host
}

func (hostAddress *HostAddress) GetPort() int {
	return hostAddress.port
}
