var EventEmitter = require('events').EventEmitter;

module.exports = createFakeKafkaProducer;

function createFakeKafkaProducer(settings) {
    var topics = settings.topics;

    return function FakeKafkaProducer(topic, opts) {
        var producer = new EventEmitter();
        producer.messages = [];

        var port = topics[topic];
        if (typeof port === 'function') {
            var observ = port;
            port = observ();
            observ(function (value) {
                port = value;
            });
        }

        producer.connect = function (cb) {
            process.nextTick(function () {
                if (!port) {
                    var err = new Error('ECONNRESET');

                    if (cb) cb(err);
                    return producer.emit('error', err);
                }

                if (cb) cb();
                producer.emit('connect');
            });
        };

        producer.send = function (message, cb) {
            if (settings.shouldKafkaSendFail) {
                var bool = settings.shouldKafkaSendFail(message);
                if (bool) {
                    return cb(new Error('ECONNRESET'));
                }
            }

            producer.messages.push(message);

            process.nextTick(cb);
        };

        return producer;
    };
}

