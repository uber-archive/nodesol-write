var test = require('tape');
var cuid = require('cuid');

var NodeSol = require('../../lib/nodesol.js').NodeSol;
var startZkKafka = require('../lib/start-zk-kafka.js');
var killZkKafka = require('../lib/kill-zk-kafka.js');

/* 
    - spawn zookeeper.
    - create NodeSol
    - connect()
    - produce
    - consume

*/
test('writing to real zookeeper', function (assert) {
    /*jshint camelcase: false*/

    startZkKafka(function (err, procs) {
        assert.ifError(err);

        if (err) {
            killZkKafka(procs);
            return assert.end();
        }

        var ns = procs.ns = new NodeSol({
            host: 'localhost',
            port: 2181
        });
        ns.connect(function (err) {
            // fetch producer to get NodeSol client to behave
            ns.get_producer('rt-New_York');

            ns.create_consumer(cuid(), 'rt-New_York', {}, function (stream) {
                if (!stream) {
                    return assert.fail('no stream');
                }

                stream.once('data', function (chunk) {

                    assert.equal(String(chunk), 'some msg');

                    stream.kafka_stop();
                    stream.client.socket.destroy();
                    killZkKafka(procs);
                    assert.end();
                });

                ns.produce('rt-New_York', 'some msg');
            });

            
        });
    });
});
