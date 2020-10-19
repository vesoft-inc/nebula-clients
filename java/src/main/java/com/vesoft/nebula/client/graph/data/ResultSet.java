/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.data;

import com.vesoft.nebula.graph.ErrorCode;
import com.vesoft.nebula.graph.ExecutionResponse;
import java.util.List;

public class ResultSet {
    private int code = ErrorCode.SUCCEEDED;
    private String errorMessage;

    /**
     * Constructor
     */
    public ResultSet() {
        this(new ExecutionResponse());
    }

    public ResultSet(ExecutionResponse resp) {
        if (resp.error_msg != null) {
            errorMessage = new String(resp.error_msg).intern();
        }

        code = resp.error_code;
    }

    public boolean isSucceeded() {
        return this.code == ErrorCode.SUCCEEDED;
    }

    public int getErrorCode() {
        return this.code;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }
}
