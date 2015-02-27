var should = require('should');
var assert = require('assert');
var mock = require('nodemock');
var async = require('async');
var SandboxedModule = require('sandboxed-module');
var EventEmitter = require('events').EventEmitter;
var buffertools = require('buffertools');


describe('NodeSol', function() {
    var ns_module;
    var NodeSol;
    var prozess;
    var zookeeper;

    describe('broker discovery', function() {
        it('should respect broker_reconnect_after option', function(done) {
            var ns = new NodeSol({broker_reconnect_after: 6741});
            ns.broker_reconnect_after.should.equal(6741);
            ns.connect(function() {
                var producer = ns.get_producer('test_topic');
                producer.reconnect_after.should.equal(6741);
                done();
            });
        });

        it('should return a producer for a broker', function(done) {
            var ns = new NodeSol();
            ns.connect(function() {
                var producer = ns.get_producer('test_topic');
                should.exist(producer);
                done();
            });
        });

        it('should return same producer for same topic', function(done) {
            var ns = new NodeSol();
            ns.connect(function() {
                var producer = ns.get_producer('test_topic');
                should.exist(producer);
                var other_producer = ns.get_producer('test_topic');
                should.exist(other_producer);
                assert.equal(producer, other_producer);
                done();
            });
        });

        it('should create a topic a topic if it does not exist', function(done) {
            var ns = new NodeSol();
            ns.connect(function() {
                var producer = ns.get_producer('other_topic');
                should.exist(producer);
                producer.broker_host.should.equal('localhost');
                producer.broker_port.should.equal(9093);
                var other_producer = ns.get_producer('other_topic');
                assert.equal(other_producer, producer);
                done();
            });
        });

        it('should reuse same producer when reentering call', function(done) {
            var ns = new NodeSol();
            ns.connect(function() {
                var total_calls = 2;
                var producer1, producer2;
                var check_result = function() {
                    total_calls -= 1;
                    if (total_calls === 0) {
                        should.exist(producer1);
                        should.exist(producer2);
                        assert.equal(producer1, producer2);
                        done();
                    }
                };
                var producer = ns.get_producer('other_topic');
                producer1 = producer;
                check_result();
                ns.get_producer('other_topic');
                producer2 = producer;
                check_result();
            });
        });
    });

    describe('kafka producer', function() {
        var ns;
        beforeEach(function(done) {
            ns = new NodeSol({broker_reconnect_after: 0});
            ns.connect(done);
        });

        it('should have topic when created', function(done) {
            var producer = ns.get_producer('test_topic');
            should.exist(producer);
            producer.topic.should.equal('test_topic');
            done();
        });

        it('should send a message to specified topic', function(done) {
            ns.produce('test_topic', 'test_message', function(err) {
                process.nextTick(function() {
                    should.not.exist(err);
                    kafka_mock.messages[0].should.equal('test_message');
                    done();
                });
            });
        });
        it('produce should take objects as well', function(done) {
            ns.produce('test_topic', {test: 'message', number: 3}, function(err) {
                process.nextTick(function() {
                    should.not.exist(err);
                    kafka_mock.messages[0].should.equal("{\"test\":\"message\",\"number\":3}");
                    done();
                });
            });
        });

        it('should log message to specified topic', function(done) {
            ns.log_line('test_topic', 'test_message', function(err) {
                process.nextTick(function() {
                    should.not.exist(err);
                    kafka_mock.messages[0].should.equal("{\"ts\":1369945301.743,\"host\":\"test_host\",\"msg\":\"test_message\"}", function() {});
                    done();
                });
            });
        });
        it('should log a byte buffer to specified topic', function(done) {
            var buf = new Buffer([0x3, 0x4, 0x23, 0x42]);
            ns.produce('test_buffer', buf, function(err) {
                process.nextTick(function() {
                    should.not.exist(err);
                    assert(buffertools.compare(kafka_mock.messages[0], buf)===0);
                    done();
                });
            });
        });
    });
    describe('kafka producer with shouldAddTopicToMessage set', function() {
        var ns;
        beforeEach(function(done) {
            ns = new NodeSol({broker_reconnect_after: 0, shouldAddTopicToMessage: true});
            ns.connect(done);
        });

        it('should log topic in message', function(done) {
            ns.log_line('test_topic', 'test_message', function(err) {
                process.nextTick(function() {
                    kafka_mock.messages[0].should.equal(
                        "{\"ts\":1369945301.743,\"host\":\"test_host\",\"msg\":\"test_message\",\"topic\":\"test_topic\"}", 
                        function() {}
                    );
                    done();
                });
            });
        });
    });

    describe('failed message queue', function() {
        var ns;
        beforeEach(function(done) {
            ns = new NodeSol({
                broker_reconnect_after: 0,
                queue_limit: 5
            });
            ns.connect(done);
        });

        it('should queue failed messages into a memory queue', function(done) {
            ns.produce('test_topic', 'failed_message', function() {
                process.nextTick(function() {
                    should.equal(ns.producers.test_topic.queue.first(), 'failed_message');
                    done();
                });
            });
        });

        it('should send queued messages after a successful reconnect', function(done) {
            var orig_fn = kafka_mock.send;

            kafka_mock.send = function(message, callback) {
                callback('ECONNRESET');
            };
            ns.produce('test_topic', 'test_message_one', delay_10(function(err) {
                kafka_mock.send = orig_fn;
                ns.produce('test_topic', 'test_message_two', delay_10(function(err) {
                    should.not.exist(err);
                    kafka_mock.messages[0].should.equal('test_message_one');
                    kafka_mock.messages[1].should.equal('test_message_two');
                    done();
                }));
            }));
        });

        it('should retry queued messages on resend failure', function(done) {
            var orig_send_fn = kafka_mock.send;
            var send_count = 1;
            var send_one_and_fail = function(message, callback) {
                if (send_count > 0) {
                    send_count -= 1;
                    orig_send_fn.apply(kafka_mock, [message, callback]);
                }
                else {
                    callback('ECONNRESET');
                }
            };
            kafka_mock.send = function(message, callback) {
                callback('ECONNRESET');
            };
            ns.produce('test_topic', 'test_message_one', delay_10(function(err) {
                ns.produce('test_topic', 'test_message_two', delay_10(function(err) {
                    kafka_mock.send = send_one_and_fail;
                    ns.produce('test_topic', 'test_message_three', delay_10(function(err) {
                        kafka_mock.send = orig_send_fn;
                        ns.produce('test_topic', 'test_message_four', delay_10(function(err) {
                            should.not.exist(err);
                            kafka_mock.messages[0].should.equal('test_message_one');
                            kafka_mock.messages[1].should.equal('test_message_two');
                            kafka_mock.messages[2].should.equal('test_message_three');
                            kafka_mock.messages[3].should.equal('test_message_four');
                            ns.get_queue_size('test_topic').should.equal(0);
                            done();
                        }));
                    }));
                }));
            }));
        });

        it('should obey message limit and drop oldest messages first', function(done) {
            // message queue size is 5 per topic
            var orig_send_fn = kafka_mock.send;
            var fail_count = 7;
            var fail_first_seven = function(message, callback) {
                if(fail_count > 0) {
                    fail_count -= 1;
                    callback('ECONNRESET');
                }
                else {
                    orig_send_fn.apply(kafka_mock, [message, callback]);
                }
            };
            kafka_mock.send = fail_first_seven;

            async.times(7, function(err, next) {
                ns.produce('test_topic', 'test_message', next);
            }, function(err) {
                ns.produce('test_topic', 'test_message', delay_10(function(err) {
                    should.not.exist(err);
                    kafka_mock.messages.length.should.equal(5);
                    ns.get_queue_size('test_topic').should.equal(0);
                    done();
                }));
            });
        });

        it('should report queue size', function(done) {
            for (var i = 0; i < 5; i++) {
                ns.produce('test_topic', 'failed_message');
            }
            ns.get_queue_size('test_topic').should.equal(5);
            done();
        });
    });

    it('should not schedule new reconnects', function(done) {
        kafka_mock = mock.mock('connect').times(2);
        kafka_mock.send = function(message, callback) {
            if (kafka_mock.messages === undefined) {
                kafka_mock.messages = [];
            }
            process.nextTick(callback.bind(callback, ['ECONRESET']));
        }

        kafka_mock._events = new EventEmitter();
        kafka_mock.once = function(evt, callback) {
            var events = kafka_mock._events;
            events.once(evt, callback);
            if (evt !== 'error') {
                process.nextTick(function() {
                    events.emit(evt);
                });
            }
        };

        // XXX|SZ: this actually relies on timing which is bad
        var nodesol = new NodeSol({broker_reconnect_after: 7});
        nodesol.connect();

        nodesol.produce('test_topic', 'test_message', function() {
            nodesol.produce('test_topic', 'test_message2', delay_10(function() {
                kafka_mock.assert().should.equal(true);
                done();
            }));
        });
    });

    var delay_10 = function(callback) {
        return function() {
            setTimeout(callback, 10);
        };
    };

    beforeEach(function (done) {
        prozess = build_kafka_module();
        zookeeper = build_zookeeper_module();
        ns_module = SandboxedModule.load('../lib/nodesol', {
            requires: {
                'zookeeper-uber': zookeeper,
                'prozess-uber': prozess,
                'os': { hostname: function() { return 'test_host'; }}
            },
            globals: {
                'Date': function() { return new Date(1369945301743); }
            }
        });

        NodeSol = ns_module.exports.NodeSol;
        done();
    });

    var zk_mock; // moved here for easier inspection
    var build_zookeeper_module = function() {
        zk_mock = mock.mock('connect').takes(function() {}).calls(0);
        zk_mock = zk_mock.mock('a_get_children').takes('/brokers/topics', null, function() {}).calls(2, [0, 'ok', ['test_topic', 'dummy_topic']]);
        zk_mock = zk_mock.mock('a_get_children').takes('/brokers/topics/test_topic', null, function() {}).calls(2, [0, 'ok', ['0']]);
        zk_mock = zk_mock.mock('a_get_children').takes('/brokers/topics/dummy_topic', null, function() {}).calls(2, [0, 'ok', ['1']]);
        zk_mock = zk_mock.mock('a_get_children').takes('/brokers/ids', null, function() {}).calls(2, [0, 'ok', ['0', '1']]);
        zk_mock = zk_mock.mock('a_get').takes('/brokers/ids/0', null, function() {}).calls(2, [0, 'ok', {}, ['identifier1:127.0.0.1:9262']]);
        zk_mock = zk_mock.mock('a_get').takes('/brokers/ids/1', null, function() {}).calls(2, [0, 'ok', {}, ['identifier2:127.0.0.1:9263']]);
        var module = function () {
            return zk_mock;
        };
        return module;
    };

    var kafka_mock;
    var build_kafka_module = function() {
        kafka_mock = mock.mock('connect').times(100);
        kafka_mock.send = function(message, callback) {
            if (typeof(kafka_mock.messages) == 'undefined') {
                kafka_mock.messages = [];
            }
            if (['failed_message'].some(function(item, index, array) {
                    return item == message;
                })) {
                    callback('ECONNRESET');
            } else {
                kafka_mock.messages.push(message);
                process.nextTick(callback);
            }
        };
        kafka_mock.ignore('on');
        kafka_mock.raise_error = function(err) {
            kafka_mock._events.emit(evt, err);
        };
        kafka_mock._events = new EventEmitter();
        kafka_mock.once = function(evt, callback) {
            var events = kafka_mock._events;
            events.once(evt, callback);
            if (evt !== 'error') {
                process.nextTick(function() {
                    events.emit(evt);
                });
            }
        };

        var module = {
            Producer: function() {
                kafka_mock._events.removeAllListeners('error');
                //kafka_mock._events.removeAllListeners('connect');
                return kafka_mock;
            }
        };

        return module;
    };
});
