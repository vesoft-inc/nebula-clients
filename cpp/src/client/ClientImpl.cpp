/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <memory>

#include "./ClientImpl.h"

namespace nebula {

bool ClientImpl::open(const std::string &address, int32_t port, uint32_t timeout) {
    auto socket = apache::thrift::async::TAsyncSocket::newSocket(
        folly::EventBaseManager::get()->getEventBase(), address, port, timeout);

    client_ = std::make_unique<graph::cpp2::GraphServiceAsyncClient>(
        apache::thrift::HeaderClientChannel::newChannel(socket));
    return true;
}

AuthResponse ClientImpl::authenticate(const std::string &user, const std::string &password) {
    if (client_ == nullptr) {
        return AuthResponse{ErrorCode::E_DISCONNECTED};
    }

    graph::cpp2::AuthResponse resp;
    try {
        client_->sync_authenticate(resp, user, password);
    } catch (const std::exception &ex) {
        return AuthResponse{ErrorCode::E_RPC_FAILURE, -1};
    }
    if (resp.__isset.session_id) {
        sessionId_ = *resp.get_session_id();
    }
    return from(resp);
}

ExecutionResponse ClientImpl::execute(int64_t sessionId, const std::string &stmt) {
    if (client_ == nullptr) {
        return ExecutionResponse{ErrorCode::E_DISCONNECTED, 0};
    }

    graph::cpp2::ExecutionResponse resp;
    try {
        client_->sync_execute(resp, sessionId, stmt);
    } catch (const std::exception &ex) {
        return ExecutionResponse{ErrorCode::E_RPC_FAILURE, 0};
    }

    return from(resp);
}

void ClientImpl::signout() {
    if (client_ != nullptr) {
        client_->sync_signout(sessionId_);
    }
}

/*static*/ ErrorCode ClientImpl::from(graph::cpp2::ErrorCode code) {
    switch (code) {
        case graph::cpp2::ErrorCode::SUCCEEDED:
            return ErrorCode::SUCCEEDED;
        case graph::cpp2::ErrorCode::E_DISCONNECTED:
            return ErrorCode::E_DISCONNECTED;
        case graph::cpp2::ErrorCode::E_FAIL_TO_CONNECT:
            return ErrorCode::E_FAIL_TO_CONNECT;
        case graph::cpp2::ErrorCode::E_RPC_FAILURE:
            return ErrorCode::E_RPC_FAILURE;
        case graph::cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD:
            return ErrorCode::E_BAD_USERNAME_PASSWORD;
        case graph::cpp2::ErrorCode::E_SESSION_INVALID:
            return ErrorCode::E_SESSION_INVALID;
        case graph::cpp2::ErrorCode::E_SESSION_TIMEOUT:
            return ErrorCode::E_SESSION_TIMEOUT;
        case graph::cpp2::ErrorCode::E_SYNTAX_ERROR:
            return ErrorCode::E_SYNTAX_ERROR;
        case graph::cpp2::ErrorCode::E_EXECUTION_ERROR:
            return ErrorCode::E_EXECUTION_ERROR;
        case graph::cpp2::ErrorCode::E_STATEMENT_EMTPY:
            return ErrorCode::E_STATEMENT_EMTPY;
        case graph::cpp2::ErrorCode::E_USER_NOT_FOUND:
            return ErrorCode::E_USER_NOT_FOUND;
        case graph::cpp2::ErrorCode::E_BAD_PERMISSION:
            return ErrorCode::E_BAD_PERMISSION;
        case graph::cpp2::ErrorCode::E_SEMANTIC_ERROR:
            return ErrorCode::E_SEMANTIC_ERROR;
    }
}

/*static*/ AuthResponse ClientImpl::from(graph::cpp2::AuthResponse &resp) {
    if (resp.__isset.session_id) {
        return AuthResponse{from(resp.get_error_code()), *resp.get_session_id()};
    } else {
        return AuthResponse{from(resp.get_error_code()), -1};
    }
}

/*static*/ ExecutionResponse ClientImpl::from(graph::cpp2::ExecutionResponse &resp) {
    return ExecutionResponse{from(resp.get_error_code()),
                             resp.get_latency_in_us(),
                             resp.get_data() != nullptr
                                 ? std::make_unique<DataSet>(std::move(*resp.get_data()))
                                 : nullptr,
                             resp.get_space_name() != nullptr
                                 ? std::make_unique<std::string>(std::move(*resp.get_space_name()))
                                 : nullptr,
                             resp.get_error_msg() != nullptr
                                 ? std::make_unique<std::string>(std::move(*resp.get_error_msg()))
                                 : nullptr,
                             resp.get_comment() != nullptr
                                 ? std::make_unique<std::string>(std::move(*resp.get_comment()))
                                 : nullptr};
}

}   // namespace nebula
