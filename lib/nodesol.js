var kafka = require('prozess-uber'),
    async = require('async'),
    CBuffer = require('CBuffer'),
    Class = require('uberclass'),
    os = require('os'),
    Buffer = require('buffer').Buffer;

var QueueProducer = Class.extend({

    DISCONNECTED: 0,
    RECONNECT_SCHEDULED: 1,
    CONNECTING: 2,
    CONNECTED: 3,

    init: function(topic, broker_host, broker_port, options) {
        var self = this;
        options = options || {};
        self.topic = topic;
        self.broker_host = broker_host;
        self.broker_port = broker_port;
        self.queue_limit = options.queue_limit || 1000;
        self.connectionCache = 'connectionCache' in options ?
            options.connectionCache : true;

        if (options.reconnect_after !== undefined) {
            self.reconnect_after = options.reconnect_after;
        }
        else {
            self.reconnect_after = 1000;
        }

        self.queue = new CBuffer(self.queue_limit);

        self.state = self.DISCONNECTED;
        self.connect();
    },

    schedule_reconnect: function() {
        var self = this;
        if (self.state === self.DISCONNECTED) {
            self.state = self.RECONNECT_SCHEDULED;
            setTimeout(function() {
                self.connect();
            }, self.reconnect_after);
        }
    },

    connect: function(callback) {
        var self = this;

        callback = callback || function() {};

        // clean up previous connection if any
        if (self.connection && self.connection.connection && self.connection.connection.destroy) {
            self.connection.connection.destroy();
        }

        self.connection = new kafka.Producer(self.topic, {
            host: self.broker_host,
            port: self.broker_port,
            connectionCache: self.connectionCache
        });

        self.connection.once('connect', function() {
            self.state = self.CONNECTED;
            self.flush_queue(self.error_handler(callback));
        });
        self.connection.once('error', function(err) {
            if (self.state === self.CONNECTING) {
                self.state = self.DISCONNECTED;
            }
            self.schedule_reconnect();
            callback(err);
        });

        self.state = self.CONNECTING;
        self.connection.connect();
    },

    enqueue: function(message, callback) {
        var self = this;

        self.queue.push(message);

        callback();
    },

    flush_queue: function(callback) {
        var self = this;
        var error = null;
        async.whilst(
            function() {
                return !error && self.queue.size > 0;
            },
            function(cb) {
                var message = self.queue.shift();
                self.connection.send(message, function(err) {
                    if (err) {
                        error = err;
                        self.queue.unshift(message);
                    }
                    cb(err);
                });
            },
            function() {
                callback(error);
            }
        );
    },

    error_handler: function(callback) {
        var self = this;
        callback = callback || function() {};
        return function(err) {
            if (err) {
                if(self.state === self.CONNECTED) {
                    self.state = self.DISCONNECTED;
                    self.schedule_reconnect();
                }
            }
            callback(err);
        };
    },

    produce: function(message, callback) {
        var self = this;
        callback = callback || function() {};
        if (self.state === self.CONNECTED) {
            return self.connection.send(message, self.error_handler(function(err) {
                if (err) {
                    self.enqueue(message, callback);
                } else {
                    callback();
                }
            }));
        }
        else {
            self.enqueue(message, callback);
            self.schedule_reconnect();
        }
    },

    get_queue_size: function() {
        return this.queue.size;
    }
});


var NodeSol = Class.extend({
    init: function(options) {
        var self = this;
        options = options || {};

        self.leafHost = options.leafHost || 'localhost';
        self.leafPort = options.leafPort || 9093;

        // just store and pass to producer whatever's there
        self.broker_reconnect_after = options.broker_reconnect_after;
        self.queue_limit = options.queue_limit;
        self.connectionCache = options.connectionCache;

        self.producers = {};

        self.local_hostname = os.hostname();
        self.shouldAddTopicToMessage = options.shouldAddTopicToMessage || false;
    },

    connect: function(callback) {
        if (!callback) {
            callback = function() {};
        }
      
        callback();
    },

    get_producer: function(topic) {
        var self = this;

        if(self.producers[topic]) {
            return self.producers[topic];
        }
        else {
            var producer = new QueueProducer(
                topic,
                self.leafHost,
                self.leafPort, 
                {
                    reconnect_after: self.broker_reconnect_after,
                    queue_limit: self.queue_limit,
                    connectionCache: self.connectionCache
                }
            );
            self.producers[topic] = producer;
            return producer;
        }
    },

    get_queue_size: function(topic) {
        var self = this;
        if(self.producers[topic]) {
            return self.producers[topic].get_queue_size();
        }
        else {
            return null;
        }
    },

    produce: function(topic, message, callback) {
        var self = this;

        if (!callback) {
            callback = function() {};
        }

        if (typeof(message) !== 'string' && !Buffer.isBuffer(message)) {
            message = JSON.stringify(message);
        }

        var producer = self.get_producer(topic);
        if (!producer) {
            return callback(new Error("No brokers available."));
        }

        return producer.produce(message, callback);
    },

    log_line: function(topic, message, callback) {
        var self = this;
        callback = callback || function() {};
        var wholeMessage = {ts: (new Date()).getTime()/1000.0, host: self.local_hostname, msg: message};
        if (self.shouldAddTopicToMessage) {
            wholeMessage.topic = topic;
        }
        self.produce(topic, wholeMessage, function(err) {
            callback(err);
        });
    }
});

exports.NodeSol = NodeSol;
