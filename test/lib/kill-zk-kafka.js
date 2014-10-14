var exec = require('child_process').exec;

var destroyNodesol = require('./destroy-nodesol.js');

module.exports = cleanup;

function cleanup(procs) {
    if (procs.ns) {
        destroyNodesol(procs.ns);
    }

    var killed = false;

    if (procs.kafka) {
        procs.kafka.on('exit', function () {
            // console.log('exited kafka', arguments);
            
            maybeKillZk();
        });

        procs.kafka.on('close', function () {
            // console.log('kafka close');
        });

        if (procs.kafka.pid) {
            exec('kafka-server-stop.sh');
        } else {
            maybeKillZk();
        }
    } else {
        maybeKillZk();
    }

    function maybeKillZk(err) {
        if (killed) {
            return;
        }

        killed = true;

        if (procs.zk) {
            procs.zk.on('exit', function () {
                // console.log('exited zk', arguments);
            });

            procs.zk.on('close', function () {
                // console.log('zk close');
            });

            if (procs.zk.pid) {
                exec('zookeeper-server-stop.sh');
            }
        }
    }
}
