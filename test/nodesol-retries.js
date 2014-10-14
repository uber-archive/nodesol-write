/*jshint camelcase: false*/
var test = require('tape');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('should retry queued messages', function (assert) {
    var kafkaFail = 0; // fail everything

    var ns = createNodeSol({
        queue_limit: 5,
        broker_reconnect_after: 0,
        shouldKafkaSendFail: function () {
            if (kafkaFail === 0) {
                return false;
            }

            kafkaFail--;
            return true;
        }
    });
    ns.connect(function (err) {
        assert.ifError(err);

        kafkaFail = Infinity; // fail everything.
        ns.produce('test_topic', 'test_message_one', function (err) {
            assert.ifError(err);

            var producer = ns.get_producer('test_topic');
            producer.once('reconnectScheduled', onMsgOne);
        });
    });

    function onMsgOne(err) {
        assert.ifError(err);

        assert.equal(ns.get_queue_size('test_topic'), 1);

        ns.produce('test_topic', 'test_message_two', function (err) {
            assert.ifError(err);

            var producer = ns.get_producer('test_topic');
            producer.once('reconnectScheduled', onMsgTwo);
        });
    }

    function onMsgTwo(err) {
        assert.ifError(err);

        assert.equal(ns.get_queue_size('test_topic'), 2);

        kafkaFail = 1; // fail once.

        ns.produce('test_topic', 'test_message_three', function (err) {
            assert.ifError(err);

            var producer = ns.get_producer('test_topic');
            producer.once('queueDrained', onMsgThree);
        });
    }

    function onMsgThree(err) {
        assert.ifError(err);

        assert.equal(ns.get_queue_size('test_topic'), 0);
        kafkaFail = 0;
        ns.produce('test_topic', 'test_message_four', onMsgFour);
    }

    function onMsgFour(err) {
        assert.ifError(err);

        var producer = ns.get_producer('test_topic');
        var messages = producer.connection.messages;
        assert.equal(messages.length, 4);
        assert.deepEqual(messages, [
            'test_message_one',
            'test_message_two',
            'test_message_three',
            'test_message_four'
        ]);
        assert.equal(ns.get_queue_size('test_topic'), 0);

        destroyNodesol(ns);
        assert.end();
    }
});
