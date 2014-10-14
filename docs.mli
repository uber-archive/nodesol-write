import { Callback, EventEmitter } from "node.types"

type KafkaClient : EventEmitter & {
    connect: (Callback) => void,
    produce: (topic: String, message: Object | String, cb?: Callback),
    log_line: (topic: String, message: Object | String, cb?: Callback)
}

nodesol : {
    NodeSol: (opts?: {
        host?: String,
        port?: Number
    }) => KafkaClient
}
