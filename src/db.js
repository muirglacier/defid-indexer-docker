const _ = require("lodash/fp");
const { mongodb } = require("../config");
const { MongoClient } = require("mongodb");
var crypto = require("crypto");
const client = new MongoClient(mongodb.connectString);
const database = client.db("defichain");
const stats = database.collection("stats");
const txs = database.collection("txs");
const blocks = database.collection("blocks");
const accounts = database.collection("accounts");
const vaults = database.collection("vaults");
const dexprices = database.collection("dexprices");
const log = require("./logger");

var session = null;

var cachedLastStats = null;
var cachedLastStatsUncomitted = null;

var toPushTxn = [];
var toPushBlocks = [];
var toPushVault = [];
var toPushAccounts = [];
var finalToPushDexPrices = []; // these go to database
var toPushDexPrices = {}; // these aggregate duplicated within one block

const addVault = (vault) => {
  toPushVault.push(vault);
};

const addAccount = (vault) => {
  toPushAccounts.push(vault);
};

const consolidateDexPrices = () => {
  Object.keys(toPushDexPrices).forEach((key) => {
    finalToPushDexPrices.push(toPushDexPrices[key]);
  });
};

const addTx = (tx, blockHash, blockHeight) => {
  // delete some irrelevant bollocks
  delete tx["version"];
  delete tx["size"];
  delete tx["vsize"];
  delete tx["weight"];
  delete tx["hex"];

  // rectify vin and remove useless shit
  for (let i = 0; i < tx.vin.length; i++) {
    delete tx.vin[i]["sequence"];
    delete tx.vin[i]["scriptSig"];
    if ("coinbase" in tx.vin[i]) {
      tx.vin[i]["coinbase"] = "true";
    }
  }

  // rectify vout and remove useless shit
  for (let i = 0; i < tx.vout.length; i++) {
    if ("addresses" in tx.vout[i].scriptPubKey) {
      tx.vout[i]["recipient"] = tx.vout[i].scriptPubKey.addresses[0];
    } else {
      tx.vout[i]["data"] = "true";
    }
    delete tx.vout[i]["scriptPubKey"];
  }

  tx["blockHash"] = blockHash;
  tx["blockHeight"] = blockHeight;

  toPushTxn.push(tx);

  // record dex price if PoolSwap transaction
  if ("customTx" in tx && tx.customTx.type == "PoolSwap") {
    tx.reserve_changes.forEach((elem) => {
      const price = elem.newReserveA / elem.newReserveB;
      const price_reverse = elem.newReserveB / elem.newReserveA;
      const volume_a = Math.abs(elem.newReserveA - elem.oldReserveA);
      const volume_b = Math.abs(elem.newReserveB - elem.oldReserveB);
      const poolId = elem.poolId;

      let obj = {
        time: tx.time,
        blockHeight,
        price,
        price_reverse,
        volume_a,
        volume_b,
        poolId,
      };
      if (poolId in toPushDexPrices) {
        const old_volumina = toPushDexPrices[poolId];
        obj.volume_a += old_volumina.volume_a;
        obj.volume_b += old_volumina.volume_b;
      }

      toPushDexPrices[poolId] = obj;
    });
  }
};

const addChainLastStats = (blockHash, blockHeight) => {
  cachedLastStatsUncomitted = {
    _id: crypto.createHash("md5").update("stats").digest("hex"),
    lastHash: blockHash,
    lastHeight: blockHeight,
  };
};

const addBlock = (block) => {
  delete block["tx"];
  delete block["difficulty"];
  delete block["chainwork"];
  delete block["mediantime"];
  delete block["nextblockhash"];
  delete block["bits"];
  delete block["confirmations"];
  delete block["strippedsize"];
  delete block["size"];
  delete block["weight"];
  delete block["masternode"];
  delete block["mintedBlocks"];
  delete block["stakeModifier"];
  delete block["version"];
  delete block["versionHex"];
  delete block["merkleroot"];
  delete block["nonutxo"];

  toPushBlocks.push(block);
};

const shutup = async () => {
  await client.close();
};

const startTransaction = () => {
  toPushBlocks = [];
  toPushTxn = [];
  toPushAccounts = [];
  toPushVault = [];
  finalToPushDexPrices = [];
  toPushDexPrices = {};

  const transactionOptions = {
    readPreference: "primary",
    readConcern: { level: "local" },
    writeConcern: { w: "majority" },
    maxCommitTimeMS: 10000,
  };
  session = client.startSession();
  session.startTransaction(transactionOptions);
};

const commitTransaction = async () => {
  const query = { _id: crypto.createHash("md5").update("stats").digest("hex") };
  await stats.replaceOne(query, cachedLastStatsUncomitted, {
    session: session,
    upsert: true,
  });
  log.info(
    "Pushing",
    toPushBlocks.length,
    "blk,",
    toPushTxn.length,
    "tx",
    toPushVault.length,
    "vaults",
    toPushAccounts.length,
    "acc",
    finalToPushDexPrices.length,
    "pri"
  );

  if (toPushBlocks.length > 0)
    await blocks.insertMany(toPushBlocks, { session });
  toPushBlocks = [];
  if (toPushTxn.length > 0) await txs.insertMany(toPushTxn, { session });
  toPushTxn = [];
  if (toPushVault.length > 0) await vaults.insertMany(toPushVault, { session });
  toPushVault = [];
  if (toPushAccounts.length > 0)
    await accounts.insertMany(toPushAccounts, { session });
  toPushAccounts = [];
  if (finalToPushDexPrices.length > 0)
    await dexprices.insertMany(finalToPushDexPrices, { session });
  finalToPushDexPrices = [];
  toPushDexPrices = {};

  return session
    .commitTransaction()
    .then(() => {
      cachedLastStats = cachedLastStatsUncomitted;
      cachedLastStatsUncomitted = null;
      session.endSession();
      session = null;
    })
    .catch((err) => {
      cachedLastStats = null;
      cachedLastStatsUncomitted = null;
      throw err;
    });
};

const cleanTransaction = () => {
  cachedLastStats = null;
  toPushBlocks = [];
  toPushTxn = [];
  toPushAccounts = [];
  toPushVault = [];
  finalToPushDexPrices = [];
  toPushDexPrices = {};
};

const abortTransaction = () => {
  cleanTransaction();

  return session
    .abortTransaction()
    .then(() => {
      session.endSession();
      session = null;
    })
    .catch((err) => {
      throw err;
    });
};

const getIndexedBlockHeight = () => {
  const query = { _id: crypto.createHash("md5").update("stats").digest("hex") };

  if (cachedLastStats != null) {
    return Promise.resolve(cachedLastStats?.lastHeight || 0);
  }

  return stats
    .findOne(query)
    .then((doc) => {
      cachedLastStats = doc;
      return doc?.lastHeight || 0;
    })
    .catch((err) => {
      throw err;
    });
};

const getIndexedBlockHash = () => {
  const query = { _id: crypto.createHash("md5").update("stats").digest("hex") };

  if (cachedLastStats != null) {
    return Promise.resolve(cachedLastStats?.lastHash || "none");
  }

  return stats
    .findOne(query)
    .then((doc) => {
      cachedLastStats = doc;
      return doc?.lastHash || "none";
    })
    .catch((err) => {
      throw err;
    });
};

module.exports = {
  getIndexedBlockHeight,
  getIndexedBlockHash,
  startTransaction,
  consolidateDexPrices,
  commitTransaction,
  abortTransaction,
  cleanTransaction,
  addTx,
  addBlock,
  addChainLastStats,
  shutup,
  addAccount,
  addVault,
};
