var startZookeeper = require('./start-zk.js');
var startKafka = require('./start-kafka.js');

module.exports = startBoth;

function startBoth(callback) {
    var procs = {};
    procs.zk = startZookeeper(function (err) {
        if (err) {
            return callback(err, procs);
        }


        procs.kafka = startKafka(function (err) {
            if (err) {
                return callback(err, procs);
            }

            callback(null, procs);
        });
    });
}

