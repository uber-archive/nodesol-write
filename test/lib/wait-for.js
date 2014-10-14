module.exports = waitFor;

function waitFor(streams, phrase, cb) {
    var stdout = '';
    var stderr = '';
    var done = false;

    streams.stdout.on('data', onDataStdout);
    streams.stdout.on('end', onEnd);

    streams.stderr.on('data', onDataStderr);
    streams.stderr.on('end', onEnd);

    function onDataStdout(chunk) {
        chunk = String(chunk);
        stdout += chunk;
        if (chunk.indexOf(phrase) !== -1) {
            cleanup();

            cb(null);
        }
    }

    function onEnd() {
        if (done) {
            return;
        }

        var err = new Error('saw no information');
        err.stdout = stdout;
        err.stderr = stderr;

        cleanup();
        cb(err);
    }

    function onDataStderr(chunk) {
        chunk = String(chunk);
        stderr += chunk;
    }

    function cleanup() {
        done = true;
        streams.stdout.removeListener('data', onDataStdout);
        streams.stdout.removeListener('end', onEnd);
        streams.stderr.removeListener('data', onDataStderr);
        streams.stderr.removeListener('end', onEnd);

        stdout = '';
        stderr = '';
    }
}
