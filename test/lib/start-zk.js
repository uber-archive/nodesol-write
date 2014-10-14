var spawn = require('child_process').spawn;

var waitFor = require('./wait-for');

module.exports = startZookeeper;

function startZookeeper(cb) {
    var proc = spawn('zookeeper-server-start.sh', [
        './test/config/zookeeper.properties'
    ]);

    waitFor(proc, 'Snapshotting:', function (err) {
        if (err) {
            console.log(err.stdout);
            console.error(err.stderr);
        }
        cb(err, proc);
    });

    if (process.env.DEBUG || process.env.DEBUG_ZK) {
        proc.stdout.pipe(process.stdout);
        proc.stderr.pipe(process.stderr);
    }

    return proc;
}

