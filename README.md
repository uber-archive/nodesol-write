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


## License (MIT)

Copyright (C) 2013 by Uber Technologies, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
