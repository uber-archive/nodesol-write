/*jshint camelcase: false*/
var test = require('tape');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('discovering kafka failure', function (assert) {
    var zk = {
        connect: function (cb) {
            process.nextTick(cb);
        },
        a_get_children: function (str, _, cb) {
            if (str === '/brokers/topics') {
                return process.nextTick(function () {
                    cb(1, 'discover kafka error');
                });
            }

            return cb(0, '', []);
        }
    };

    var ns = createNodeSol({
        zk: zk
    });
    ns.connect(function (err) {
        assert.ok(err);

        assert.equal(err.message, 'discover kafka error');

        destroyNodesol(ns);
        assert.end();
    });
});

test('discover topic brokers error', function (assert) {
    var zk = {
        connect: function (cb) {
            process.nextTick(cb);
        },
        a_get_children: function (str, _, cb) {
            if (str === '/brokers/topics/0') {
                return process.nextTick(function () {
                    cb(1, 'topic brokers error');
                });
            }

            return cb(0, '', [0]);
        },
        a_get: function (str, _, cb) {
            return cb(0, '', '');
        }
    };

    var ns = createNodeSol({
        zk: zk
    });
    ns.connect(function (err) {
        assert.ok(err);

        assert.equal(err.message, 'topic brokers error');

        destroyNodesol(ns);
        assert.end();
    });
});

test('discover all brokers error', function (assert) {
    var zk = {
        connect: function (cb) {
            process.nextTick(cb);
        },
        a_get_children: function (str, _, cb) {
            if (str === '/brokers/ids') {
                return process.nextTick(function () {
                    cb(1, 'all brokers error');
                });
            }

            return cb(0, '', [0]);
        }
    };

    var ns = createNodeSol({
        zk: zk
    });
    ns.connect(function (err) {
        assert.ok(err);

        assert.equal(err.message, 'all brokers error');

        destroyNodesol(ns);
        assert.end();
    });
});

test('discover broker id error', function (assert) {
    var zk = {
        connect: function (cb) {
            process.nextTick(cb);
        },
        a_get_children: function (str, _, cb) {
            return cb(0, '', [0]);
        },
        a_get: function (str, _, cb) {
            process.nextTick(function () {
                cb(1, 'broker id error');
            });
        }
    };

    var ns = createNodeSol({
        zk: zk
    });
    ns.connect(function (err) {
        assert.ok(err);

        assert.equal(err.message, 'broker id error');

        destroyNodesol(ns);
        assert.end();
    });
});

test('zk emits error without callback', function (assert) {
    var zk = {
        connect: function (cb) {
            process.nextTick(cb);
        },
        a_get_children: function (str, _, cb) {
            if (str === '/brokers/topics') {
                return process.nextTick(function () {
                    cb(1, 'discover kafka error');
                });
            }

            return cb(0, '', []);
        }
    };

    var ns = createNodeSol({
        zk: zk
    });
    ns.connect();

    ns.once('error', function (err) {
        assert.ok(err);

        assert.equal(err.message, 'discover kafka error');

        destroyNodesol(ns);
        assert.end();
    });
});
