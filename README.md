# uber-nodesol

A queueing Kafka/ZooKeeper client library for Nodejs.

Messages will be held in a memory queue if broker is unavailable.

ccache/f90cache break zookeeper's installation.  You will need to `CCACHE_DISABLE=1 F90CACHE_DISABLE=1 npm install` or install a different compiler than default apple gcc that works properly with ccache.

## Usage

```js
var NodeSol = require('nodesol').NodeSol;
var ns = new NodeSol({host: 'localhost', port: 2181});
ns.connect(function() {
    ns.log_line('my_topic', 'Important message'); // Timestamped log entry with host
    ns.produce('my_other_topic', 'Important message #2'); // Just sends the raw message directly to kafka
});
```

### Options

Nodesol constructor accepts these options:
  - `host` - ZooKeeper hostname (default: `localhost`)
  - `port` - ZooKeeper port (default: `2181`)
  - `timeout` - ZooKeeper connection timeout, ms (default: `200000`)
  - `broker_reconnect_after` - interval between broker reconnect attempts, ms (default: `1000`)
  - `queue_limit` - maximum number of items to hold in topic queue when broker is unavailable (default: `1000`)

## Running tests

Tests are run using `npm`:

    npm test

## MIT Licenced
