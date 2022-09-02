#!/usr/bin/env node
const Indexer = require('../src/indexer')
const log = require('../src/logger')

const idx = Indexer()

idx.getCustomTx("e804d3f250a8074cd77a4711a2d7d880d422a6086741fb5389aca29ae47b7b26", "80073ed391bf6564a9e482ac1716577ee6a8c093a4c1709f72abbe2fac28d3a8")
.then(last => {
  log.info('Last block height is', last)
  process.exit()
})
.catch(e => {
  log.error(e)
})