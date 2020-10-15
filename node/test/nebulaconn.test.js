/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

var GraphTtypes = require('../nebula/interface/graph_types');
var NebulaConn = require("../nebula/net/NebulaConn").NebulaConn
const assert = require('assert');
const expect = require('chai').expect

describe('#nebula connection', () => {
    it('test open succeeded', () => {
        var conn = new NebulaConn('localhost', 3699, 1000)
        try {
            conn.open()
            conn.close()
        } catch (e) {
            assert(false, 'Expected open succeeded, but:' + e.stack)
        }

    })

    it('test authenticate', (done) => {
        var conn = new NebulaConn('localhost', 3699, 1000)
        try {
            conn.open()
            conn.authenticate('root', 'nebula', function (response) {
                assert(response.success.error_code == 0)
                assert(response.success.session_id != 0)
                conn.signout(response.success.session_id)
                done()
                conn.close()
            })
        } catch (e) {
            conn.close()
            assert(false, 'Expected open succeeded, but: ' + e.stack)
        }

    })

    it('test execute', (done) => {
        var conn = new NebulaConn('localhost', 3699, 1000)
        try {
            conn.open()
            conn.authenticate('root', 'nebula', function (response) {
                assert(response.success.error_code == 0)
                assert(response.success.session_id != 0)
                sessionId = response.success.session_id
                console.info('response.success.session_id = ' + sessionId)
                conn.execute(sessionId, 'SHOW SPACES', function (response) {
                    assert(response.success.error_code == 0)
                    assert(response.success.data.column_names[0] === undefined)
                    assert(response.success.data.rows.length == 0)
                    done()
                    conn.close()

                })
            })

        } catch (e) {
            conn.close()
            assert(false, 'Expected open succeeded, but: ' + e.stack)
        }

    })

    it('test open failed', () => {
        var conn = new NebulaConn('localhost', 3999, 1000)
        try {
            conn.open()
            conn.close()
        } catch (e) {
            assert(true, 'Expected open failed, but: ' + e.stack)
        }

    })
})
