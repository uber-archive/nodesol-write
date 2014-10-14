var FakeZookeeper = require('./fake-zookeeper');
var createFakeKafkaProducer =
    require('./fake-kafka-producer.js');

var NodeSol = require('../../lib/nodesol.js').NodeSol;

module.exports = createNodeSol;

function createNodeSol(opts) {
    opts = opts || {};
    opts.topics = opts.topics || {
        'test_topic': '9262'
    };

    opts.zk = opts.zk || FakeZookeeper(opts);
    opts.createProducer = createFakeKafkaProducer(opts);

    return new NodeSol(opts);
}
