/*jshint camelcase: false*/
var test = require('tape');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('Nodesol returns a producer', function (assert) {
    var ns = createNodeSol();
    ns.connect(function (err) {
        assert.ifError(err);

        var producer = ns.get_producer('test_topic');
        assert.ok(producer);

        destroyNodesol(ns);
        assert.end();
    });
});

test('NodeSol returns same producer for same topic', function (assert) {
    var ns = createNodeSol();
    ns.connect(function (err) {
        assert.ifError(err);

        var producer = ns.get_producer('test_topic');
        assert.ok(producer);
        var otherProducer = ns.get_producer('test_topic');
        assert.ok(otherProducer);
        assert.equal(producer, otherProducer);

        destroyNodesol(ns);
        assert.end();
    });
});

test('should create topic if not exist', function (assert) {
    var ns = createNodeSol();
    ns.connect(function (err) {
        assert.ifError(err);

        var producer = ns.get_producer('other_topic');
        assert.ok(producer);
        assert.equal(producer.broker_host, 'localhost');
        assert.equal(producer.broker_port, '9262');

        var otherProducer = ns.get_producer('other_topic');
        assert.equal(producer, otherProducer);

        destroyNodesol(ns);
        assert.end();
    });
});

test('producer has topics', function (assert) {
    var ns = createNodeSol();
    ns.connect(function (err) {
        assert.ifError(err);

        var producer = ns.get_producer('test_topic');
        assert.ok(producer);
        assert.equal(producer.topic, 'test_topic');

        destroyNodesol(ns);
        assert.end();
    });
});
