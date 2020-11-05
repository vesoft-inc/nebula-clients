/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <memory>

#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "common/interface/gen-cpp2/GraphServiceAsyncClient.h"

#include "nebula/client/Response.h"

namespace nebula {

// Wrap the thrift client
class ClientImpl {
public:
    ClientImpl() = default;

    ~ClientImpl() = default;

    bool open(const std::string &address, int32_t port, uint32_t timeout);

    AuthResponse authenticate(const std::string &user, const std::string &password);

    ExecutionResponse execute(int64_t sessionId, const std::string &stmt);

    void signout(int64_t sessionId);

private:
    static ErrorCode from(graph::cpp2::ErrorCode code);

    static AuthResponse from(graph::cpp2::AuthResponse &resp);

    static ExecutionResponse from(graph::cpp2::ExecutionResponse &resp);

    std::unique_ptr<graph::cpp2::GraphServiceAsyncClient> client_{nullptr};
};

}   // namespace nebula
