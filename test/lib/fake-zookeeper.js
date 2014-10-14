/*jshint camelcase: false*/
var Router = require('routes');
var cuid = require('cuid');

module.exports = FakeZookeeper;

function FakeZookeeper(opts) {
    var topics = opts.topics || {};

    var topicNames = Object.keys(topics);
    var ports = Object.keys(topics).map(function (k) {
        if (typeof topics[k] === 'function') {
            return topics[k]();
        }

        return topics[k];
    });
    var getChildrenRouter = Router();
    getChildrenRouter.addRoute('/brokers/topics', function () {
        return topicNames;
    });
    getChildrenRouter.addRoute('/brokers/topics/:id', function (params) {
        return [topicNames.indexOf(params.id)];
    });
    getChildrenRouter.addRoute('/brokers/ids', function () {
        return Object.keys(topicNames);
    });

    var getRouter = Router();
    getRouter.addRoute('/brokers/ids/:id', function (params) {
        return ports[params.id];
    });

    return {
        connect: function (cb) {
            process.nextTick(cb);
        },
        a_get_children: function (path, watch, cb) {
            process.nextTick(function () {
                var match = getChildrenRouter.match(path);
                if (!match) {
                    return cb(1, 'not ok');
                }

                var children = match.fn(match.params);
                // console.log('a_get_children', path, children);
                cb(0, 'ok', children);
            });
        },
        a_get: function (path, watch, cb) {
            process.nextTick(function () {
                var match = getRouter.match(path);
                if (!match) {
                    return cb(1, 'not ok');
                }

                var port = match.fn(match.params);

                var stat = {};
                var parts = [cuid(), 'localhost', port];
                var data = parts.join(':');
                // console.log('a_get', path, data);
                cb(0, 'ok', stat, data);
            });
        }
    };
}
