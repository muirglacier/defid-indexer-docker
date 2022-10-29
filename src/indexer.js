const _ = require("lodash/fp");
const process = require("process");

const config = require("../config");
const db = require("./db");
const btc = require("./rpc");
const log = require("./logger");
const { execPath } = require("process");
const { time } = require("console");

const IDLE_BETWEEN_BLOCKS = _.get("index.idleBetweenBlocks", config);
const IDLE_BETWEEN_TXS = _.get("index.idleBetweenTxs", config);
const STARTING_BLOCK_HEIGHT = _.get("index.startingBlockHeight", config);
const MONITOR_IDLE_TIME = _.get("index.monitorIdleTime", config); // checks for new block every 5 seconds
const BLOCK_GROUPING = _.get("index.blockGrouping", config);

let pushCounter = 0;
let timeout = null;

const Indexer = (options) => {
  /**
   * Get the last block height from blockchain
   *
   * @name getBtcBlockHeight
   * @function
   * @returns {Promise<Number>} Last block height
   */
  const getBtcBlockHeight = async () => {
    return btc("getblockchaininfo").then(_.get("blocks"));
  };

  /**
   * Get custom tx
   *
   * @name getVout
   * @function
   * @returns {Vin<CustomTx>} Vin
   */
  const getVout = (txid) => {
    return btc("getrawtransaction", [txid, true]);
  };

  /**
   * Get custom tx
   *
   * @name getAccountsForBlock
   * @function
   * @returns {something} Vin
   */
  const getAccountsForBlock = (blockHeight) => {
    return btc("getaccountsforblock", [blockHeight.toString()]);
  };

  /**
   * Get custom tx
   *
   * @name getVaultsForBlock
   * @function
   * @returns {something} Vin
   */
  const getVaultsForBlock = (blockHeight) => {
    return btc("getvaultsforblock", [blockHeight.toString()]);
  };

  /**
   * Get custom tx
   *
   * @name getSpecialsForBlock
   * @function
   * @returns {something} Vin
   */
  const getSpecialsForBlock = (blockHeight) => {
    return btc("getspecialsforblock", [blockHeight.toString()]);
  };

  /**
   * Get custom tx
   *
   * @name getStateChange
   * @function
   * @returns {something} Vin
   */
  const getStateChange = (txid, blockHeight) => {
    return btc("getundo", [txid, blockHeight.toString()]);
  };

  /**
   * Get custom tx
   *
   * @name getCustomTx
   * @function
   * @returns {Promise<CustomTx>} CustomTx
   */
  const getCustomTx = async (txid, blockhash) => {
    return btc("getcustomtx", [txid, blockhash]);
  };

  /**
   * Extract and save OP_RETURN metadata for
   * a transaction.
   *
   * @name saveMeta
   * @function
   * @param {Object} tx Transaction object
   * @param {String} blockHash Block hash
   * @param {Number} blockHeight Block Height
   * @returns {Promise}
   */
  const saveMeta = async (tx, blockHash, blockHeight, blockTime, n) => {
    tx["time"] = blockTime;
    tx["n"] = n;

    if (
      tx.txid ==
      "522954ffdc481b6259f730b10626b945cd479d0b6f1e1b9f7b93cf72a233c6db"
    )
      console.log(tx);

    await getCustomTx(tx.txid, blockHash)
      .then((txcustom) => {
        if (
          tx.txid ==
          "522954ffdc481b6259f730b10626b945cd479d0b6f1e1b9f7b93cf72a233c6db"
        ) {
          console.log("retreived custom:");
          console.log(txcustom);
        }
        if (txcustom?.valid == true) {
          delete txcustom["blockHash"];
          delete txcustom["blockHeight"];
          delete txcustom["blockTime"];
          delete txcustom["confirmations"];
          delete txcustom["valid"];
          tx["customTx"] = txcustom;
        }
        return getStateChange(tx.txid, blockHeight);
      })
      .then((state) => {
        if (
          tx.txid ==
          "522954ffdc481b6259f730b10626b945cd479d0b6f1e1b9f7b93cf72a233c6db"
        ) {
          console.log("retreived state change:");
          console.log(state);
        }
        tx["state"] = state;
      })
      .catch((err) => {}); // todo

    // now fix up the vins with proper sender addresses
    let vinvalues = 0;
    for (var i = 0; i < tx.vin.length; ++i) {
      if ("txid" in tx.vin[i]) {
        await getVout(tx.vin[i].txid)
          .then((prev) => {
            if ("addresses" in prev.vout[tx.vin[i].vout].scriptPubKey) {
              tx.vin[i]["sender"] =
                prev.vout[tx.vin[i].vout].scriptPubKey.addresses[0];
            } else {
              tx.vin[i]["data"] = "true";
            }
            tx.vin[i]["value"] = prev.vout[tx.vin[i].vout].value;
            vinvalues += prev.vout[tx.vin[i].vout].value;
          })
          .catch((err) => {
            throw err;
          });
      }
    }

    if (
      tx.txid ==
      "522954ffdc481b6259f730b10626b945cd479d0b6f1e1b9f7b93cf72a233c6db"
    )
      console.log("filled in VIN values");

    // now calculate fee paid, and store
    let voutvalues = 0;
    for (var i = 0; i < tx.vout.length; ++i) {
      voutvalues += tx.vout[i].value;
    }
    tx["fee"] = vinvalues - voutvalues;

    await db.addTx(tx, blockHash, blockHeight).catch((e) => {
      throw e;
    });
  };

  /**
   * Iterate of a list of transactions,
   * extract and save all OP_RETURN Metatags
   *
   * @name indexTxs
   * @function
   * @param {Array<Object>} txs Transactions array
   * @param {String} blockHash
   * @param {String} blockHeight
   * @returns {Promise<Object>} { totalIndexed }
   */
  const indexTxs = async (txs, blockHash, blockHeight, blockTime) => {
    // Parse txs array sequentially

    let rejected = false;
    for (let x = 0; x < txs.length; ++x) {
      let tx = txs[x];

      if (rejected) break;

      if (
        tx.txid ==
        "522954ffdc481b6259f730b10626b945cd479d0b6f1e1b9f7b93cf72a233c6db"
      )
        console.log("Starting marker:", tx.txid);
      // Extract and save all metatags for
      // this transaction (if found)
      await saveMeta(tx, blockHash, blockHeight, blockTime, x)
        .then(() => {})
        .catch((err) => {
          log.error("Failed indexing tx:", tx.txid);
          log.error(err);
          rejected = true;
          throw err;
        });
    }

    const totalIndexed = txs.length;
    if (!rejected) return { success: true, totalIndexed };
  };

  /**
   * Save sequentially all metadata given a blockHeight
   *
   * This will fetch the block of the blockHeight
   * And then iterate over block's transactions to
   * save sequentially all found metadata of the transactions.
   *
   * @name indexBlock
   * @function
   * @param {Number} blockHeight Block height to save for
   * @returns {Promise<Object>} Return the total meta indexed
   */
  const indexBlock = async (blockHeight) => {
    let globBlock = undefined;
    await btc("getblockhash", [blockHeight])
      .then((hash) => btc("getblock", [hash, 2]))
      .then((block) => {
        globBlock = block;
        _bl = JSON.parse(JSON.stringify(block));
        db.addBlock(_bl);
        db.addChainLastStats(block.hash, blockHeight);
        return db.preFillMainPoolsFromDB();
      })
      .then(() =>
        indexTxs(globBlock.tx, globBlock.hash, blockHeight, globBlock.time)
      )
      .then(() => getSpecialsForBlock(blockHeight))
      .then((state) => {
        if (!Array.isArray(state)) {
          throw "specials object was not an array";
        }

        let nullid =
          "0000000000000000000000000000000000000000000000000000000000000000";
        let fakestate = { balance_changes: [], main_pools: [] };

        // todo
        if (state.length > 0) {
          state.forEach((element) => {
            fakestate.balance_changes.push({
              owner: element.owner,
              token: element.token,
              new_amount: element.new_value,
            });
          });

          // create fake TX for block-specials
          let faketx = {
            txid: nullid,
            specialType: 1,
            specials: state,
            vin: [],
            vout: [],
            time: globBlock.time,
            state: fakestate,
            n: 100000,
          };

          return db.addSpecialTx(faketx, globBlock.hash, blockHeight);
        } else {
          return Promise.resolve();
        }
      })
      .then(() => getAccountsForBlock(blockHeight))
      .then((state) => {
        if (!Array.isArray(state)) {
          throw "vault object was not an array";
        }
        state.forEach((element) => {
          db.addAccount(element);
        });
      })
      .catch((err) => {
        log.error(`Failed to index all metadata for ${blockHeight}.`, err);
        throw err;
      });
  };

  /**
   * Given a range of blocks heights,
   * start indexing sequentially all their metadata.
   *
   * @name indexBlocks
   * @function
   * @param {Number/String} startBlockHeight Block to start indexing
   * @param {Number/String} endBlockHeight  Block to stop indexing (including)
   */
  const indexBlocks = async (startBlockHeight, endBlockHeight) => {
    let start = _.parseInt(10, startBlockHeight);
    let end = endBlockHeight ? _.parseInt(10, endBlockHeight) : start;

    let times = _.add(_.subtract(end, start), 1);

    log.info("Syncing blocks.");
    for (let idx = 0; idx < times; ++idx) {
      // start new mongodb transaction for bulk writes
      if (idx == 0) {
        try {
          db.startTransaction();
        } catch (e) {
          db.abortTransaction();
          throw e;
        }
      }

      // idx starts from 0, will include startingBlock
      const nextBlock = _.add(start, idx);
      await indexBlock(nextBlock);

      pushCounter++;
      if (pushCounter >= BLOCK_GROUPING) {
        // push everything to the DB, and create a new session for the next batch

        try {
          await db.commitTransaction(nextBlock);
          pushCounter = 0;
        } catch (reason) {
          log.error("Fatal error during transaction comittment.");
          log.error("Error follows:");
          log.error(reason.toString());
          pushCounter = 0;

          // abort all transactions
          try {
            await db.abortTransaction();
          } catch (e) {}
          throw reason;
        }

        try {
          db.startTransaction();
        } catch (e) {
          await db.abortTransaction();
          throw e;
        }
      }
    }

    if (pushCounter >= 0) {
      try {
        await db.commitTransaction("final push");
        pushCounter = 0;
      } catch (reason) {
        log.error("Fatal error during transaction comittment.");
        log.error("Error follows:");
        log.error(reason.toString());
        pushCounter = 0;

        // abort all transactions
        try {
          await db.abortTransaction();
        } catch (e) {}

        throw reason;
      }
    }
  };

  /**
   * Get the last block that is indexed succesfully
   * (not in errored_blocks)
   *
   * @name monitor
   * @function
   * @param {} startingBlockHeight
   */
  const monitor = async () => {
    log.info("Checking for new blocks");
    // Find the last btc height
    await Promise.all([db.getIndexedBlockHeight(), getBtcBlockHeight()]).then(
      async ([indexedHeight, btcHeight]) => {
        let startBlockHeight = _.max([
          STARTING_BLOCK_HEIGHT,
          indexedHeight + 1,
        ]);
        // hacky workaround
        if (startBlockHeight == 1) startBlockHeight = 0;

        log.info(`Last indexed block height: ${indexedHeight}`);
        log.info(`Last bitcoin block height: ${btcHeight}`);
        if (startBlockHeight > btcHeight) {
          log.info("No new blocks are generated.");
          log.info("Going idle...");
          timeout = setTimeout(() => {
            monitor();
          }, MONITOR_IDLE_TIME);

          return null;
        }
        await indexBlocks(startBlockHeight, btcHeight)
          .then(() => {
            log.info("Going idle...");
            timeout = setTimeout(() => {
              monitor();
            }, MONITOR_IDLE_TIME);
          })
          .catch((e) => {
            log.info("Going idle due to error...");
            try {
              db.abortTransaction();
            } catch (e) {}
            log.error(e);
            timeout = setTimeout(() => {
              monitor();
            }, MONITOR_IDLE_TIME);
          });
      }
    );
  };

  // API
  return {
    // Expose rpc commands
    rpc: btc,
    monitor,
    getBtcBlockHeight,
    getCustomTx,
  };
};

module.exports = Indexer;
