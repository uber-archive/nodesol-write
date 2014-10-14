/*jshint camelcase: false*/
var test = require('tape');
var series = require('run-series');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('buffering is bounded', function (assert) {
    var kafkaDown = true;
    var ns = createNodeSol({
        queue_limit: 5,
        broker_reconnect_after: 1,
        shouldKafkaSendFail: function () {
            return kafkaDown;
        }
    });

    ns.connect(function (err) {
        assert.ifError(err);

        series([
            ns.produce.bind(ns, 'test_topic', 'test_message1'),
            ns.produce.bind(ns, 'test_topic', 'test_message2'),
            ns.produce.bind(ns, 'test_topic', 'test_message3'),
            ns.produce.bind(ns, 'test_topic', 'test_message4'),
            ns.produce.bind(ns, 'test_topic', 'test_message5'),
            ns.produce.bind(ns, 'test_topic', 'test_message6'),
            ns.produce.bind(ns, 'test_topic', 'test_message7')
        ], function (err) {
            kafkaDown = false;

            assert.equal(ns.get_queue_size('test_topic'), 5);

            ns.produce('test_topic', 'test_message8', function (err) {
                assert.ifError(err);

                assert.equal(ns.get_queue_size('test_topic'), 5);

                var producer = ns.get_producer('test_topic');
                producer.once('queueDrained', function () {
                    var conn = producer.connection;
                    assert.deepEqual(conn.messages, [
                        'test_message4',
                        'test_message5',
                        'test_message6',
                        'test_message7',
                        'test_message8'
                    ]);
                    assert.equal(ns.get_queue_size('test_topic'), 0);

                    destroyNodesol(ns);
                    assert.end();
                });
            });
        });
    });
});
