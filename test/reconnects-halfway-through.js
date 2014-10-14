/*jshint camelcase: false*/
var test = require('tape');
var Timer = require('time-mock');

var createProducer = require('./lib/create-producer.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('Does reconnect after eventual failure', function t(assert) {
    var kafkaDown = false;

    var timer = Timer(0);
    createProducer({
        queue_limit: 5,
        broker_reconnect_after: 20,
        shouldKafkaSendFail: function () {
            return kafkaDown;
        },
        setTimeout: timer.setTimeout
    }, 'test_topic', function (err, proc, ns){
        assert.ifError(err);

        var conn = proc.connection;

        ns.produce('test_topic', 'message1', onMessage);

        function onMessage(err) {
            assert.ifError(err);

            assert.deepEqual(conn.messages, ['message1']);

            kafkaDown = true;

            ns.produce('test_topic', 'message2', onMessage2);
        }

        function onMessage2(err) {
            assert.ifError(err);

            assert.deepEqual(conn.messages, ['message1']);
            assert.deepEqual(proc.get_queue_size(), 1);


            proc.once('reconnectError', function (err) {
                assert.ok(err);
                assert.equal(err.message, 'ECONNRESET');

                kafkaDown = false;

                onReconnect();
            });

            timer.advance(20);
        }

        function onReconnect() {
            proc.once('queueDrained', function () {
                var proc2 = ns.get_producer('test_topic');
                var conn2 = proc.connection;

                assert.equal(proc2.get_queue_size(), 0);
                assert.deepEqual(conn2.messages, ['message2']);
                assert.equal(proc, proc2);
                assert.notEqual(conn, conn2);

                destroyNodesol(ns);
                assert.end();
            });

            timer.advance(20);
        }
    });
});

