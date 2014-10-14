module.exports = destroyNodesol;

function destroyNodesol(ns) {
    Object.keys(ns.producers).forEach(function (key) {
        var producer = ns.producers[key];
        // QueueProducer
        if (producer &&
            // Kafka.Producer
            producer.connection &&
            // WritableStream
            producer.connection.connection &&
            // net.Socket
            producer.connection.connection._connection
        ) {
            producer.connection.connection._connection.destroy();
        }

        if (producer &&
            producer.reconnectTimer
        ) {
            clearTimeout(producer.reconnectTimer);
            producer.reconnectTimer = null;
            producer.state = producer.DISCONNECTED;
        } else if (producer &&
            producer.state === producer.CONNECTING
        ) {
            producer.once('reconnectScheduled', function () {
                clearTimeout(producer.reconnectTimer);
                producer.reconnectTimer = null;
                producer.state = producer.DISCONNECTED;
            });
        }
    });

    if (ns.zk && ns.zk.destroy) {
        ns.zk.destroy();
    } else if (ns.zk && ns.zk.close) {
        ns.zk.close();
    }
}
