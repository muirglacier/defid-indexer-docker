#!/usr/bin/env node
const Indexer = require('../src/indexer')
const log = require('../src/logger')

const idx = Indexer()

log.info('Start monitoring...')
idx.monitor()
.catch(e => {
  log.error(e)
})