var kafka = require('prozess'),
    ZooKeeper = require('zookeeper-uber'),
    async = require('async'),
    CBuffer = require('CBuffer'),
    stream = require('readable-stream'),
    Class = require('uberclass'),
    util = require('util'),
    os = require('os');


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

    // params: broker_id, consumer_id, topic
    CONSUMER_OFFSETS_PATH: '/sortsol/consumer-offsets/%s/%s/%s',

    init: function(options) {
        var self = this;
        options = options || {};

        self.host = options.host || 'localhost';
        self.port = options.port || 2181;
        self.timeout = options.timeout || 200000;
        self.debug_level = options.debug_level || ZooKeeper.ZOO_LOG_LEVEL_WARNING;

        // just store and pass to producer whatever's there
        self.broker_reconnect_after = options.broker_reconnect_after;
        self.queue_limit = options.queue_limit;
        self.connectionCache = options.connectionCache;

        self.topic_brokers = {};
        self.brokers = {};
        self.producers = {};
        self.queue = {};

        self.local_hostname = os.hostname();

        self.zk = null;
    },

    discover_kafka: function(callback) {
        var self = this;
        self.topic_brokers = {};
        self.zk.a_get_children('/brokers/topics', null, function(rc, err, topics) {
            if (topics) {
                async.map(topics, self.discover_topic_brokers.bind(self), callback);
            }
            else {
                callback();
            }
        });
    },

    discover_topic_brokers: function(topic, cb) {
        var self = this;
        self.zk.a_get_children('/brokers/topics/' + topic, null, function(rc, err, brokers) {
            async.map(brokers, self.discover_broker.bind(self, topic), cb);
        });
    },

    discover_all_brokers: function(cb) {
        var self = this;

        self.zk.a_get_children('/brokers/ids', null, function(rc, err, broker_ids) {
            if (broker_ids) {
                async.map(broker_ids, function(broker_id, callback) {
                    self.zk.a_get('/brokers/ids/' + broker_id, null, function(rc, err, stat, broker_str) {
                        var parts = broker_str.toString().split(':');
                        // broker hostname is stored as creator:host:port
                        if (parts.length < 3) {
                            return callback();
                        }
                        var hostname = parts[1];
                        var port = parts[2];
                        self.brokers[broker_id] = {host: hostname, port: port, id: broker_id};
                        callback();
                    });
                }, cb);
            }
        });
    },

    discover_broker: function(topic, broker_id, cb) {
        var self = this;
        if (!self.topic_brokers[topic]) {
            self.topic_brokers[topic] = [];
        }
        self.topic_brokers[topic].push(self.brokers[broker_id]);
        cb();
    },

    connect: function(callback) {
        var self = this;
        if (!callback) {
            callback = function() {};
        }
        self.zk = new ZooKeeper({
            connect: self.host + ':' + self.port,
            timeout: self.timeout,
            debug_level: self.debug_level,
            host_order_deterministic: false
        });

        self.zk.connect(function(err) {
            if (err) {
                if (callback) {
                    callback(err);
                }
                return;
            }
            async.series([
                    self.discover_all_brokers.bind(self),
                    self.discover_kafka.bind(self)
                ], callback);
        });
    },

    get_broker_info: function(topic, create) {
        var self = this;
        create = create || false;
        if (create && (!Array.isArray(self.topic_brokers[topic]) || self.topic_brokers[topic].length === 0)) {
            self.topic_brokers[topic] = Object.keys(self.brokers).map(function(key) {
                return self.brokers[key];
            });
        }
        if (self.topic_brokers[topic]) {
            return self.topic_brokers[topic][0] || null;
        } else {
            return null;
        }
    },

    get_producer: function(topic) {
        var self = this;

        if(self.producers[topic]) {
            return self.producers[topic];
        }
        else {
            var broker_info = self.get_broker_info(topic, true);
            if (broker_info) {
                var producer = new QueueProducer(topic, broker_info.host, broker_info.port, {
                    reconnect_after: self.broker_reconnect_after,
                    queue_limit: self.queue_limit,
                    connectionCache: self.connectionCache
                });
                self.producers[topic] = producer;
                return producer;
            }
            else {
                return null;
            }
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

        if (typeof(message) !== 'string') {
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
        self.produce(topic, {ts: (new Date()).getTime()/1000.0, host: self.local_hostname, msg: message}, function(err) {
            callback(err);
        });
    }
});

exports.NodeSol = NodeSol;
