/*jshint camelcase: false*/
var test = require('tape');
var os = require('os');

var createProducer = require('./lib/create-producer.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('Can produce message', function (assert) {
    createProducer('test_topic', function (err, producer, ns) {
        assert.ifError(err);

        ns.produce('test_topic', 'test_message', function (err) {
            assert.ifError(err);

            var messages = producer.connection.messages;
            assert.deepEqual(messages, ['test_message']);

            destroyNodesol(ns);
            assert.end();
        });
    });
});

test('Can produce objects', function (assert) {
    createProducer('test_topic', function (err, producer, ns) {
        assert.ifError(err);

        ns.produce('test_topic', { test: 'message', number: 3 },
            function (err) {
                assert.ifError(err);

                var messages = producer.connection.messages;
                assert.deepEqual(messages, [
                    JSON.stringify({ test: 'message', number: 3 })
                ]);

                destroyNodesol(ns);
                assert.end();
            });
    });
});

test('can log lines to topic', function (assert) {
    createProducer('test_topic', function (err, producer, ns) {
        assert.ifError(err);

        var ts = Date.now() / 1000;

        ns.log_line('test_topic', 'test_message', function (err) {
            assert.ifError(err);

            var messages = producer.connection.messages;
            assert.equal(messages.length, 1);
            var message = messages[0];
            var json = JSON.parse(message);

            assert.equal(json.host, os.hostname());
            assert.equal(json.msg, 'test_message');
            assert.ok(ts <= json.ts);

            destroyNodesol(ns);
            assert.end();
        });
    });
});
