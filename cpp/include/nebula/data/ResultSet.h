/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/graph/Response.h"

namespace nebula {

class ResultSet {
public:
    ResultSet(ExecutionResponse &&resp) : resp_(std::move(resp)) {}

    ErrorCode errorCode() const {
        return resp_.error_code;
    }

    const DataSet *data() const {
        return resp_.data.get();
    }

    const PlanDescription *planDescription() const {
        return resp_.plan_desc.get();
    }

private:
    ExecutionResponse resp_;
};

}   // namespace nebula
