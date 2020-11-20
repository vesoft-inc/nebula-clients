/*
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.meta;

import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TTransport;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import java.util.List;

public class AbstractMetaClient {
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 3000;
    private static final int DEFAULT_CONNECTION_RETRY_SIZE = 3;
    private static final int DEFAULT_EXECUTION_RETRY_SIZE = 3;

    protected final List<HostAndPort> addresses;
    protected final int connectionRetry;
    protected final int connectionTimeout;
    protected final int executionRetry;
    protected final int timeout;

    protected TProtocol protocol;
    protected TTransport transport;


    public AbstractMetaClient(List<HostAndPort> addresses, int timeout, int connectionTimeout,
                              int connectionRetry, int executionRetry) {
        Preconditions.checkArgument(timeout > 0);
        Preconditions.checkArgument(connectionTimeout >= 0);
        Preconditions.checkArgument(connectionRetry > 0);
        Preconditions.checkArgument(executionRetry > 0);
        for (HostAndPort address : addresses) {
            String host = address.getHostText();
            int port = address.getPort();
            if (!InetAddresses.isInetAddress(host) || (port <= 0 || port >= 65535)) {
                throw new IllegalArgumentException(String.format("%s:%d is not a valid address",
                        host, port));
            }
        }

        this.addresses = addresses;
        this.timeout = timeout;
        this.connectionTimeout = connectionTimeout;
        this.connectionRetry = connectionRetry;
        this.executionRetry = executionRetry;
    }
}
