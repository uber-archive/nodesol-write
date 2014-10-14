/*jshint camelcase: false*/
var test = require('tape');

var createNodeSol = require('./lib/create-nodesol.js');
var destroyNodesol = require('./lib/destroy-nodesol.js');

test('connection a producer emits error', function t(assert) {
    var ns = createNodeSol({
        shouldKafkaSendFail: function () {
            return true;
        }
    });
    ns.connect(function (err) {
        assert.ifError(err);

        var proc = ns.get_producer('fake_topic');
        proc.connect();

        proc.once('error', function (err) {
            assert.equal(err.message, 'ECONNRESET');

            destroyNodesol(ns);
            assert.end();
        });
    });
});
