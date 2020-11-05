/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <nebula/client/Response.h>
#include <nebula/client/Config.h>
#include <nebula/client/Connection.h>
#include <nebula/client/Init.h>

int main(int argc, char* argv[]) {
    nebula::init(&argc, &argv);

    nebula::Connection c;
    if (!c.open("127.0.0.1", 3699)) {
        return 255;
    }
    auto authResp = c.authenticate("root", "nebula");
    if (authResp.code != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Exit with error code: " << static_cast<int>(authResp.code) << std::endl;
        return static_cast<int>(authResp.code);
    }
    auto resp = c.execute(authResp.sessionId, "SHOW HOSTS");
    if (resp.code != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Exit with error code: " << static_cast<int>(resp.code) << std::endl;
        return static_cast<int>(resp.code);
    }
    std::cout << *resp.data;
    c.close();
    return 0;
}
