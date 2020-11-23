/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.storage;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.storage.StorageClient;
import com.vesoft.nebula.client.graph.storage.StorageConnPool;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageClientExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClientExample.class);

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: com.vesoft.nebula.storage.StorageClientExample "
                    + "<meta_server_ip> <meta_server_port>");
            return;
        }

        NebulaPoolConfig config = new NebulaPoolConfig();
        List<HostAndPort> metaAddress = Arrays.asList(HostAndPort.fromParts(args[0],
                Integer.valueOf(args[1])));
        StorageConnPool pool = null;
        try {
            pool = new StorageConnPool(config, metaAddress);
        } catch (TException e) {
            LOGGER.error("failed to get pool,", e);
            return;
        }
        try {
            StorageClient client = pool.getStorageClient();
        } catch (Exception e) {
            LOGGER.error("failed to get client,", e);
        }
    }
}
