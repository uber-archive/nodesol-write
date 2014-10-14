/*jshint camelcase: false*/
var test = require('tape');
var observ = require('observ');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('should queue failed messages', function (assert) {
    var ns = createNodeSol();

    ns.connect(function (err) {
        assert.ifError(err);

        ns.produce('fake_topic', 'failed_message', function (err2) {
            assert.ifError(err2);

            assert.ok(ns.producers.fake_topic);
            assert.equal(ns.producers.fake_topic.queue.first(),
                'failed_message');

            var proc = ns.get_producer('fake_topic');
            proc.once('reconnectError', function (err) {
                assert.equal(err.message, 'ECONNRESET');

                destroyNodesol(ns);
                assert.end();
            });
        });
    });
});

test('should flush queue after reconnect', function (assert) {
    var topicPort = observ(null);

    var ns = createNodeSol({
        topics: {
            'test_topic': '9262',
            // fake_topic is null by default
            'fake_topic': topicPort
        },
        queue_limit: 5
    });
    ns.connect(function (err) {
        assert.ifError(err);

        ns.produce('fake_topic', 'test_message_one', function (err) {
            assert.ifError(err);

            // flip fake topic into having a port
            topicPort.set('9263');
            ns.produce('fake_topic', 'test_message_two', function (err) {
                assert.ifError(err);

                var producer = ns.get_producer('fake_topic');
                producer.once('queueDrained', function () {
                    assert.deepEqual(producer.connection.messages, [
                        'test_message_one',
                        'test_message_two'
                    ]);

                    destroyNodesol(ns);
                    assert.end();
                });
            });
        });
    });
});
