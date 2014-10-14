var createNodeSol = require('./create-nodesol');

module.exports = createProducer;

function createProducer(opts, topic, cb) {
    if (typeof opts === 'string') {
        cb = topic;
        topic = opts;
        opts = {};
    }

    var ns = createNodeSol(opts);
    ns.connect(function (err) {
        if (err) {
            return cb(err);
        }

        /*jshint camelcase: false*/
        var producer = ns.get_producer(topic);
        producer.once('connect', function () {
            cb(null, producer, ns);
        });
    });
}
