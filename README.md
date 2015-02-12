# uber-nodesol-write

A queueing Kafka client library for Nodejs.

Messages will be held in a memory queue if broker is unavailable.

This kafka library only supports writing to kafka. If you want to read
from kafka please use https://github.com/uber/nodesol

## Usage

```js
var NodeSol = require('uber-nodesol-write').NodeSol;
var ns = new NodeSol({ leafHost: 'localhost', leafPort: 9093 });

ns.connect(function() {
    ns.log_line('my_topic', 'Important message'); // Timestamped log entry with host
    ns.produce('my_other_topic', 'Important message #2'); // Just sends the raw message directly to kafka
});
```

### Options

Nodesol constructor accepts these options:
  - `leafHost` - ZooKeeper hostname (default: `localhost`)
  - `leafPort` - ZooKeeper port (default: `9093`)
  - `timeout` - ZooKeeper connection timeout, ms (default: `200000`)
  - `connectionCache` - if set to `true` it will cache TCP sockets per host+port. (default: `false`)
  - `broker_reconnect_after` - interval between broker reconnect attempts, ms (default: `1000`)
  - `queue_limit` - maximum number of items to hold in topic queue when broker is unavailable (default: `1000`)

## Running tests

Tests are run using `npm`:

    npm test

## MIT Licenced
