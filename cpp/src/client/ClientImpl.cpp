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
        return AuthResponse{ErrorCode::E_DISCONNECTED};
    }

    graph::cpp2::AuthResponse resp;
    try {
        client_->sync_authenticate(resp, user, password);
    } catch (const std::exception &ex) {
        return AuthResponse{ErrorCode::E_RPC_FAILURE, -1};
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

void ClientImpl::asyncExecute(int64_t sessionId,
                              const std::string &stmt,
                              Connection::ExecuteCallback cb) {
    client_->future_execute(sessionId, stmt).thenValue([cb = std::move(cb)](auto &&resp) {
        cb(from(resp));
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
    return ExecutionResponse{
        from(resp.get_error_code()),
        resp.get_latency_in_us(),
        resp.get_data() != nullptr ? std::make_unique<DataSet>(std::move(*resp.get_data()))
                                   : nullptr,
        resp.get_space_name() != nullptr
            ? std::make_unique<std::string>(std::move(*resp.get_space_name()))
            : nullptr,
        resp.get_error_msg() != nullptr
            ? std::make_unique<std::string>(std::move(*resp.get_error_msg()))
            : nullptr,
        resp.get_plan_desc() != nullptr
            ? std::make_unique<PlanDescription>(from(std::move(*resp.get_plan_desc())))
            : nullptr,
        resp.get_comment() != nullptr
            ? std::make_unique<std::string>(std::move(*resp.get_comment()))
            : nullptr};
}

/*static*/ PlanDescription ClientImpl::from(graph::cpp2::PlanDescription &&pd) {
    return PlanDescription{from<PlanNodeDescription>(std::move(pd).get_plan_node_descs()),
                           std::move(pd).get_node_index_map(),
                           std::move(pd).get_format()};
}

/*static*/ PlanNodeDescription ClientImpl::from(graph::cpp2::PlanNodeDescription &&pnd) {
    return PlanNodeDescription{
        std::move(pnd).get_name(),
        std::move(pnd).get_id(),
        std::move(pnd).get_output_var(),
        pnd.get_description() != nullptr
            ? std::make_unique<std::vector<Pair>>(from<Pair>(std::move(*pnd.get_description())))
            : nullptr,
        pnd.get_profiles() != nullptr ? std::make_unique<std::vector<ProfilingStats>>(
                                            from<ProfilingStats>(std::move(*pnd.get_profiles())))
                                      : nullptr,
        pnd.get_branch_info() != nullptr
            ? std::make_unique<PlanNodeBranchInfo>(from(std::move(*pnd.get_branch_info())))
            : nullptr,
        pnd.get_dependencies() != nullptr
            ? std::make_unique<std::vector<int64_t>>(std::move(*pnd.get_dependencies()))
            : nullptr};
}

/*static*/ Pair ClientImpl::from(graph::cpp2::Pair &&p) {
    return Pair{std::move(p.key), std::move(p.value)};
}

/*static*/ ProfilingStats ClientImpl::from(graph::cpp2::ProfilingStats &&pfs) {
    return ProfilingStats{pfs.get_rows(),
                          pfs.get_exec_duration_in_us(),
                          pfs.get_total_duration_in_us(),
                          pfs.get_other_stats() != nullptr
                              ? std::make_unique<std::unordered_map<std::string, std::string>>(
                                    std::move(*pfs.get_other_stats()))
                              : nullptr};
}

/*static*/ PlanNodeBranchInfo ClientImpl::from(graph::cpp2::PlanNodeBranchInfo &&pnbi) {
    return PlanNodeBranchInfo{pnbi.get_is_do_branch(), pnbi.get_condition_node_id()};
}

}   // namespace nebula
