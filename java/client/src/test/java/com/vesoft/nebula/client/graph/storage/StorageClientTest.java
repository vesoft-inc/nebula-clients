/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import java.util.Arrays;
import java.util.List;

public class StorageClientTest {

    private StorageConnPool pool;
    private final String spaceName = "test1";
    private final String ip = "127.0.0.1";


    /**
     * init pool
     */
    public void setUp() throws Exception {
        List<HostAndPort> address = Arrays.asList(
                HostAndPort.fromParts(ip, 45500),
                HostAndPort.fromParts(ip, 45501),
                HostAndPort.fromParts(ip, 45502)
        );
        NebulaPoolConfig config = new NebulaPoolConfig();
        pool = new StorageConnPool(config, address);
    }

    public void tearDown() throws Exception {
        pool.close();
    }

    private StorageClient getClient() {
        HostAndPort addr = HostAndPort.fromParts(ip, 45500);
        StorageClient client = null;
        try {
            client = pool.getStorageClient();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }
}