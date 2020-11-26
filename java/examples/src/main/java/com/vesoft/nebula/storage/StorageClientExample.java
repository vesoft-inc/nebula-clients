/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.storage;

import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.storage.StorageClient;
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
        try {
            StorageClient client = new StorageClient(HostAndPort.fromString("192.168.8.171:45500"));
        } catch (Exception e) {
            LOGGER.error("failed to get client,", e);
        }
    }
}
