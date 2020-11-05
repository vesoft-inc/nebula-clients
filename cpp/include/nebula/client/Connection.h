/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <memory>
#include <string>

#include "nebula/client/Response.h"

namespace nebula {

// Wrap the thrift client.
class ClientImpl;

class Connection {
public:
    Connection();

    ~Connection();

    bool open(const std::string &address, int32_t port);

    AuthResponse authenticate(const std::string &user, const std::string &password);

    ExecutionResponse execute(int64_t sessionId, const std::string &stmt);

    bool isOpen();

    void close();

    bool ping();

    void signout(int64_t sessionId);

private:
    ClientImpl *client_{nullptr};
};

}   // namespace nebula
