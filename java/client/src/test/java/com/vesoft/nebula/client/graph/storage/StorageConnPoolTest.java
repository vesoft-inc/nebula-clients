/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Assert;

public class StorageConnPoolTest extends TestCase {

    private StorageConnPool pool;

    public void setUp() throws Exception {
        super.setUp();
        testPoolInit();
    }

    public void tearDown() throws Exception {
    }

    public void testFreshCache() {

    }

    public void testPoolInit() {
        // invalidate host
        try {
            List<HostAndPort> address = Arrays.asList(HostAndPort.fromString("hostname:45500"));
            NebulaPoolConfig config = new NebulaPoolConfig();
            config.setMaxTotal(20);
            config.setMaxTotalPerKey(8);
            StorageConnPool pool = new StorageConnPool(config, address);
        } catch (TException e) {
            Assert.assertFalse(true);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
        }

        // normal
        try {
            List<HostAndPort> address = Arrays.asList(
                    HostAndPort.fromParts("127.0.0.1", 45500),
                    HostAndPort.fromParts("127.0.0.1", 45501),
                    HostAndPort.fromParts("127.0.0.1", 45502)
            );
            NebulaPoolConfig config = new NebulaPoolConfig();
            pool = new StorageConnPool(config, address);
            assertEquals(pool.getNumActive(HostAndPort.fromParts("127.0.0.1", 45500)), 0);
        } catch (Exception e) {
            e.printStackTrace();
            assertFalse(true);
        }
    }


    public void testGetClient() {
        try {
            HostAndPort address = HostAndPort.fromParts("127.0.0.1", 45500);
            StorageClient client = pool.getStorageClient(address);
            assertEquals(pool.getNumActive(address), 1);

        } catch (Exception e) {
            e.printStackTrace();
            assertFalse(true);
        }


    }
}