module.exports = {
  // Database settings
  mongodb: {
    connectString: process.env.MONGODB,
  },
  // JSON-RPC settings (bitcoind)
  rpc: {
    protocol: "http", // Optional. Will be http by default
    host: process.env.HOST, // Will be 127.0.0.1 by default
    user: process.env.USER, // Optional, only if auth needed
    password: process.env.PASS, // Optional. Mandatory if user is passed.
    port: 8554,
  },
  // Indexing settings
  index: {
    // The starting block height when monitoring.
    startingBlockHeight: 0,
    // Idle time between transactions
    idleBetweenTxs: 0, // ms
    // Idle time between blocks
    idleBetweenBlocks: 1, // ms
    // blocks between db write operations
    blockGrouping: 250,
    monitorIdleTime: 5000,
  },
};
