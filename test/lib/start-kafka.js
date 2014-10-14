var spawn = require('child_process').spawn;

var waitFor = require('./wait-for');

module.exports = startKafka;

function startKafka(cb) {
    var proc = spawn('kafka-server-start.sh', [
        './test/config/server.properties'
    ]);

    waitFor(proc, 'Kafka server started.', function (err) {
        if (err) {
            console.log(err.stdout);
            console.error(err.stderr);
        }
        cb(err, proc);
    });

    if (process.env.DEBUG || process.env.DEBUG_KAFKA) {
        proc.stdout.pipe(process.stdout);
        proc.stderr.pipe(process.stderr);
    }

    return proc;
}
