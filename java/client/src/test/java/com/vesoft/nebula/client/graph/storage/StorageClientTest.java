/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.google.common.net.HostAndPort;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class StorageClientTest {

    private final String ip = "127.0.0.1";

    public void setUp() {
    }

    public void tearDown() {
    }

    @Test
    public void testClient() {
        List<HostAndPort> address = Arrays.asList(
                HostAndPort.fromParts(ip, 45500),
                HostAndPort.fromParts(ip, 45501),
                HostAndPort.fromParts(ip, 45502)
        );
        StorageClient client = new StorageClient(address);
        try {
            client.connect();
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }
}