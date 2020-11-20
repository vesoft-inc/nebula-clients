/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.google.common.net.HostAndPort;
import junit.framework.TestCase;

public class StorageConnectionTest extends TestCase {
    private HostAndPort address = HostAndPort.fromParts("127.0.0.1", 32804);
    private int timeout = 0;
    private StorageConnection connection;

    public void setUp() throws Exception {
        super.setUp();
        connection = new StorageConnection();

    }

    public void tearDown() throws Exception {
    }

    public void testOpen() {
        try {
            connection.open(address, timeout);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            assertFalse(true);
        }
    }

}