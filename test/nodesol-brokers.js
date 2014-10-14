/*jshint camelcase: false*/
var test = require('tape');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('NodeSol discovers a broker', function (assert) {
    var ns = createNodeSol();
    ns.connect(function (err) {
        assert.ifError(err);

        assert.equal(ns.topic_brokers.test_topic.length, 1);

        destroyNodesol(ns);
        assert.end();
    });
});

test('NodeSol respects broker_reconnect_after', function (assert) {
    var ns = createNodeSol({
        broker_reconnect_after: 6471
    });

    assert.equal(ns.broker_reconnect_after, 6471);

    ns.connect(function (err) {
        assert.ifError(err);

        var producer = ns.get_producer('test_topic');
        assert.equal(producer.reconnect_after, 6471);

        destroyNodesol(ns);
        assert.end();
    });
});
