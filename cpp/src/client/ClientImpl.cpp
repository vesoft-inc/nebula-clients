/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include <memory>

#include "./ClientImpl.h"

namespace nebula {

bool ClientImpl::open(const std::string &address, int32_t port, uint32_t timeout) {
    try {
        auto socket = apache::thrift::async::TAsyncSocket::newSocket(
            folly::EventBaseManager::get()->getEventBase(), address, port, timeout);

        client_ = std::make_unique<graph::cpp2::GraphServiceAsyncClient>(
            apache::thrift::HeaderClientChannel::newChannel(socket));
    } catch (const std::exception &) {
        return false;
    }
    return true;
}

void ClientImpl::close() {
    if (client_ != nullptr) {
        static_cast<apache::thrift::ClientChannel *>(client_->getChannel())->closeNow();
    }
}

AuthResponse ClientImpl::authenticate(const std::string &user, const std::string &password) {
    if (client_ == nullptr) {
        return AuthResponse{
            ErrorCode::E_DISCONNECTED, nullptr, std::make_unique<std::string>("Not open connection.")};
    }

    AuthResponse resp;
    try {
        client_->sync_authenticate(resp, user, password);
    } catch (const std::exception &ex) {
        return AuthResponse{
            ErrorCode::E_RPC_FAILURE, nullptr, std::make_unique<std::string>("Unavailable Connection.")};
    }
    return resp;
}

ExecutionResponse ClientImpl::execute(int64_t sessionId, const std::string &stmt) {
    if (client_ == nullptr) {
        return ExecutionResponse{ErrorCode::E_DISCONNECTED, 0};
    }

    ExecutionResponse resp;
    try {
        client_->sync_execute(resp, sessionId, stmt);
    } catch (const std::exception &ex) {
        return ExecutionResponse{ErrorCode::E_RPC_FAILURE, 0};
    }

    return resp;
}

void ClientImpl::asyncExecute(int64_t sessionId,
                              const std::string &stmt,
                              Connection::ExecuteCallback cb) {
    client_->future_execute(sessionId, stmt).thenValue([cb = std::move(cb)](auto &&resp) {
        cb(std::move(resp));
    });
}

std::string ClientImpl::executeJson(int64_t sessionId, const std::string &stmt) {
    if (client_ == nullptr) {
        // TODO handle error
        return "";
    }

    std::string json;
    try {
        client_->sync_executeJson(json, sessionId, stmt);
    } catch (const std::exception &ex) {
        // TODO handle error
        return "";
    }

    return json;
}

void ClientImpl::asyncExecuteJson(int64_t sessionId,
                                  const std::string &stmt,
                                  Connection::ExecuteJsonCallback cb) {
    client_->future_executeJson(sessionId, stmt).thenValue(std::move(cb));
}

void ClientImpl::signout(int64_t sessionId) {
    if (client_ != nullptr) {
        client_->sync_signout(sessionId);
    }
}

}   // namespace nebula
