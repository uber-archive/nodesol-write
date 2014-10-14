var kafka = require('prozess'),
    ZooKeeper = require('zookeeper-uber'),
    async = require('async'),
    //TODO remove CBuffer
    CBuffer = require('CBuffer'),
    stream = require('readable-stream'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter,
    os = require('os'),
    globalSetTimeout = require('timers').setTimeout;

//TODO remove all snakecase
/*jshint camelcase: false */
/*jshint maxparams: 5 */

function QueueProducer(topic, broker_host, broker_port, options) {
    var self = this;
    options = options || {};

    EventEmitter.call(this);
    self.topic = topic;
    self.broker_host = broker_host;
    self.broker_port = broker_port;
    self.createProducer = options.createProducer || kafka.Producer;
    self.queue_limit = options.queue_limit || 1000;

    if (options.reconnect_after !== undefined) {
        self.reconnect_after = options.reconnect_after;
    }
    else {
        self.reconnect_after = 1000;
    }

    //TODO never implement your own buffering. re-use
    //TODO stream.Readable and stream.Writable
    self.queue = new CBuffer(self.queue_limit);
    self.reconnectTimer = null;
    self.setTimeout = options.setTimeout || globalSetTimeout;

    self.state = self.DISCONNECTED;
    self.connect(onConnectError);

    function onConnectError(error) {
        if (error) {
            self.emit('reconnectError', error);
        }
    }
}

util.inherits(QueueProducer, EventEmitter);
var proto = QueueProducer.prototype;

proto.DISCONNECTED = 0;
proto.RECONNECT_SCHEDULED = 1;
proto.CONNECTING = 2;
proto.CONNECTED = 3;

proto.schedule_reconnect = function() {
    var self = this;
    if (self.state === self.DISCONNECTED) {
        self.state = self.RECONNECT_SCHEDULED;
        self.reconnectTimer = self.setTimeout(function() {
            self.reconnectTimer = null;
            self.connect(onConnectError);
        }, self.reconnect_after);
        self.emit('reconnectScheduled');
    }

    function onConnectError(err) {
        if (err) {
            self.emit('reconnectError', err);
        }
    }
};

proto.connect = function(callback) {
    var self = this;

    if (!callback) {
        callback = function (err) {
            if (err) {
                self.emit('error', err);
            }
        };
    }

    //TODO don't reconnect. Either error or call callback()
    //TODO as already open.
    // clean up previous connection if any
    if (self.connection && self.connection.connection && self.connection.connection.destroy) {
        self.connection.connection.destroy();
    }

    self.connection = new self.createProducer(self.topic, {
        host: self.broker_host,
        port: self.broker_port,
        connectionCache: true
    });

    //TODO remove buffering. prozess already buffers
    self.connection.once('connect', function() {
        self.state = self.CONNECTED;
        self.emit('connect');
        self.flush_queue(self.error_handler(callback));
    });
    //TODO does this only error once ?
    self.connection.once('error', function(err) {
        if (self.state === self.CONNECTING) {
            self.state = self.DISCONNECTED;
        }
        //TODO remove reconnect. prozess already reconnects
        self.schedule_reconnect();
        callback(err);
    });

    self.state = self.CONNECTING;
    self.connection.connect();
};

proto.enqueue = function(message, callback) {
    var self = this;

    self.queue.push(message);

    if (callback) {
        callback(null);
    }
};

proto.flush_queue = function(callback) {
    var self = this;
    var error = null;
    //TODO rewrite as a series instead of whilst
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
            if (!error) {
                self.emit('queueDrained');
            }

            callback(error);
        }
    );
};

proto.error_handler = function(callback) {
    var self = this;
    return function(err) {
        if (err) {
            //TODO remove duplicate reconnection logic
            if(self.state === self.CONNECTED) {
                self.state = self.DISCONNECTED;
                self.schedule_reconnect();
            }
        }
        callback(err);
    };
};

proto.produce = function(message, callback) {
    var self = this;
    if (self.state === self.CONNECTED) {
        return self.connection.send(message, self.error_handler(function(err) {
            //TODO don't swallow errors. Handle a subset of errors
            //TODO as retries, not all of them
            if (err) {
                self.enqueue(message, callback);
            } else if (callback) {
                callback(null);
            }
        }));
    }
    else {
        self.enqueue(message, callback);
        self.schedule_reconnect();
    }
};

proto.get_queue_size = function() {
    return this.queue.size;
};

function NodeSol(options) {
    if (!(this instanceof NodeSol)) {
        return new NodeSol(options);
    }

    var self = this;
    options = options || {};

    EventEmitter.call(self);

    self.host = options.host || 'localhost';
    self.port = options.port || 2181;
    self.timeout = options.timeout || 200000;
    self.debug_level = options.debug_level || ZooKeeper.ZOO_LOG_LEVEL_WARNING;

    // just store and pass to producer whatever's there
    self.broker_reconnect_after = options.broker_reconnect_after;
    self.queue_limit = options.queue_limit;

    self.topic_brokers = {};
    self.brokers = {};
    self.producers = {};
    self.queue = {};

    self.local_hostname = os.hostname();

    self.zk = options.zk || null;
    self.setTimeout = options.setTimeout || globalSetTimeout;
    self.createProducer = options.createProducer;
}

util.inherits(NodeSol, EventEmitter);

var nproto = NodeSol.prototype;

// params: broker_id, consumer_id, topic
nproto.CONSUMER_OFFSETS_PATH = '/sortsol/consumer-offsets/%s/%s/%s';

nproto.discover_kafka = function(callback) {
    var self = this;
    self.topic_brokers = {};
    self.zk.a_get_children('/brokers/topics', null, function (rc, err, topics) {
        if (rc !== 0) {
            return callback(ZookeeperError(rc, err));
        }

        if (topics) {
            async.map(topics, self.discover_topic_brokers.bind(self), callback);
        }
        else {
            callback();
        }
    });
};

nproto.discover_topic_brokers = function(topic, cb) {
    var self = this;
    self.zk.a_get_children('/brokers/topics/' + topic, null, function(rc, err, brokers) {
        if (rc !== 0) {
            return cb(ZookeeperError(rc, err));
        }

        brokers = brokers || [];
        //TODO remove async, this is synchronous.
        async.map(brokers, self.discover_broker.bind(self, topic), cb);
    });
};

nproto.discover_all_brokers = function(cb) {
    var self = this;

    self.zk.a_get_children('/brokers/ids', null, function(rc, err, broker_ids) {
        if (rc !== 0) {
            return cb(ZookeeperError(rc, err));
        }

        if (broker_ids) {
            async.map(broker_ids, function(broker_id, callback) {
                self.zk.a_get('/brokers/ids/' + broker_id, null, function(rc, err, stat, broker_str) {
                    if (rc !== 0) {
                        return cb(ZookeeperError(rc, err));
                    }

                    broker_str = broker_str || '';

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
};

nproto.discover_broker = function(topic, broker_id, cb) {
    var self = this;
    if (!self.topic_brokers[topic]) {
        self.topic_brokers[topic] = [];
    }
    self.topic_brokers[topic].push(self.brokers[broker_id]);
    cb();
};

nproto.connect = function(callback) {
    var self = this;
    if (!callback) {
        callback = function (err) {
            if (err) {
                self.emit('error', err);
            }
        };
    }
    //TODO replace with pure JS ZooKeeper client
    self.zk = self.zk || new ZooKeeper({
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
};

nproto.get_broker_info = function(topic, create) {
    var self = this;
    create = create || false;
    //TODO why is this created when its fetch from zookeeper
    if (create && (!Array.isArray(self.topic_brokers[topic]) || self.topic_brokers[topic].length === 0)) {
        self.topic_brokers[topic] = Object.keys(self.brokers).map(function(key) {
            return self.brokers[key];
        });
    }
    //TODO why is it an array if only [0] is used
    if (self.topic_brokers[topic]) {
        return self.topic_brokers[topic][0] || null;
    } else {
        return null;
    }
};

//TODO do callers handle the `null` case ?
nproto.get_producer = function(topic, callback) {
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
                createProducer: self.createProducer,
                setTimeout: self.setTimeout
            });
            self.producers[topic] = producer;
            return producer;
        }
        else {
            return null;
        }
    }
};

//TODO what is this method used for ?
nproto.get_queue_size = function(topic) {
    var self = this;
    if(self.producers[topic]) {
        return self.producers[topic].get_queue_size();
    }
    else {
        return null;
    }
};

nproto.produce = function(topic, message, callback) {
    var self = this;

    if (!callback) {
        callback = function (err) {
            if (err) {
                self.emit('error', err);
            }
        };
    }

    //TODO support Buffer.isBuffer(message)
    if (typeof(message) !== 'string') {
        message = JSON.stringify(message);
    }

    var producer = self.get_producer(topic);
    if (!producer) {
        //TODO callback must be async
        return callback(new Error("No brokers available."));
    }

    return producer.produce(message, callback);
};

//TODO do we need this method? Probably not
nproto.log_line = function(topic, message, callback) {
    var self = this;
    if (!callback) {
        callback = function (err) {
            if (err) {
                self.emit('error', err);
            }
        };
    }
    self.produce(topic, {ts: (new Date()).getTime()/1000.0, host: self.local_hostname, msg: message}, function(err) {
        callback(err);
    });
};

//TODO this should not be async and should return a stream
//TODO what is a consumer_id, why do you care
nproto.create_consumer = function(consumer_id, topic, options, callback) {
    var self = this;

    options = options || {};
    //TODO why is create not set to true here :/
    var broker_info = self.get_broker_info(topic);

    //TODO why is this not an error
    if (!broker_info) {
        callback(null);
    }

    var store_offset_fn = function(offset) {
        self.store_consumer_offset(broker_info.id, consumer_id, topic, offset);
    };

    if (options.start_at_head || options.offset) {
        var offset = options.start_at_head ? undefined : options.offset;
        var consumer = new TailConsumer(broker_info.host,  broker_info.port, topic, offset, store_offset_fn);
        return callback(consumer);
    }
    else {
        self.get_consumer_offset(broker_info.id, consumer_id, topic, function(err, previous_offset) {
            //TODO handle errors
            var consumer = new TailConsumer(broker_info.host, broker_info.port, topic, previous_offset, store_offset_fn);
            callback(consumer);
        });
    }
};

nproto.make_consumer_offsets_path = function(broker_id, consumer_id, topic) {
    return util.format(this.CONSUMER_OFFSETS_PATH, broker_id, consumer_id, topic);
};

nproto.get_consumer_offset = function(broker_id, consumer_id, topic, callback) {
    var self = this;
    self.zk.a_get(self.make_consumer_offsets_path(broker_id, consumer_id, topic), null, function(rc, err, stat, data) {
        //TODO wtf why is error a string
        if (err !== 'ok') {
            callback(err);
        } else {
            callback(null, data.toString());
        }
    });
};

nproto.store_consumer_offset = function(broker_id, consumer_id, topic, offset, callback) {
    //TODO handle errors in default callback
    callback = callback || function() {};
    var self = this;
    var path = self.make_consumer_offsets_path(broker_id, consumer_id, topic);
    self.ensure_path_and_set(path, offset.toString(), callback);
};

nproto.ensure_path_and_set = function(path, data, callback) {
    var self = this;
    self.zk.mkdirp(path, function(err) {
        if (err) {
            return callback(err);
        }
        self.zk.a_set(path, data, -1, function(rc, err, stat) {
            //TODO wtf why is error a string
            if (err !== 'ok') { // This comes from native code
                callback(err);
            } else {
                callback();
            }
        });
    });
};

function TailConsumer(broker_host, broker_port, topic, offset, store_offset_fn) {
    var self = this;
    stream.Readable.call(self);
    self.started = false;
    self.offset = offset;
    self.store_offset_fn = store_offset_fn;
    self.interval_id = null;
    self.polling_interval = 500; // Hardcoded for now
    self.client = new kafka.Consumer({
        host: broker_host,
        port: broker_port,
        topic: topic,
        offset: offset,
        maxMessageSize: 2 * 1024 * 1024
    });
    self.client.connect(function(err) {
        //TODO handle err
    });
    return self;
}

// XXX mixing different class construction methods is bad, gotta choose one
//TODO remove Class.extend
util.inherits(TailConsumer, stream.Readable);

TailConsumer.prototype.update_offset = function() {
    var self = this;
    if (!self.offset || self.client.offset.cmp(self.offset) !== 0) {
        self.offset = self.client.offset;
        self.store_offset_fn(self.client.offset);
    }
};

TailConsumer.prototype.kafka_start = function(callback) {
    var self = this;
    if(!self.started) {
        self.started = true;
        //TODO nope. Use _read with the callback properly
        self.interval_id = setInterval(self.consume.bind(self), self.polling_interval);
        process.nextTick(self.consume.bind(self));  // This could be called directly as well
    }
};

TailConsumer.prototype.kafka_stop = function() {
    var self = this;

    self.started = false;
    if (self.interval_id !== null) {
        clearInterval(self.interval_id);
        self.interval_id = null;
    }
};

TailConsumer.prototype.consume = function() {
    var self = this;
    var should_continue = true;

    self.client.consume(function(err, messages) {
        var i = 0;
        if(err) {
            // If we receive an error here, we stop and report error.
            self.emit('error', err);
            self.kafka_stop();
            //TODO probably dont push(null) in error case.
            self.push(null);
            return;
        }

        // Right now we don't do internal buffering and let ReadableStream
        // class handle all the buffering.
        while(i < messages.length) {
            should_continue = self.push(messages[i].payload);
            i += 1;
        }
        // Doing this at here should reduce the likelihood of saving the
        // offset and then crashing without really processing the data.
        //TODO handle error
        self.update_offset(function(err) {});

        if(should_continue) {
            // If we received some messages, there may be more and we should send a fetch
            // right away. If not - lets wait our poll_interval until trying to fetch next batch.
            if (messages.length) {
                //TODO wtf why do we clear the interval
                clearInterval(self.interval_id);
                self.interval_id = null;
                process.nextTick(self.consume.bind(self));
            }
            else {
                //TODO use setTimeout instead of setInterval -.-
                if(!self.interval_id) {
                    self.interval_id = setInterval(self.consume.bind(self), self.polling_interval);
                }
            }
        } else {
            //TODO don't use this kafka_stop() technique
            //instead use the cb to _read
            self.kafka_stop();
        }
    });
};

TailConsumer.prototype._read = function(size) {
    this.kafka_start();
};


exports.NodeSol = NodeSol;

function ZookeeperError(rc, err) {
    var message = err || 'ZooKeeper error: ' + rc;
    return new Error(message);
}
