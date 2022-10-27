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
const allanyprices = database.collection("all-any-prices");
const btc = require("./rpc");

const log = require("./logger");
const { cursorTo } = require("readline");
const { resolveCname } = require("dns");
const logger = require("./logger");

var session = null;

var cachedLastStats = null;
var cachedLastStatsUncomitted = null;

var toPushTxn = [];
var toPushBlocks = [];
var toPushVault = [];
var toPushAccounts = [];
var finalToPushDexPrices = []; // these go to database
var toPushDexPrices = {}; // these aggregate duplicated within one block

var DFIUSDT = undefined;
var DUSDUSDT = undefined;
var DUSDDFI = undefined;

var MAPANYPOOL = {};
var TOKENTOPOOLMAP = {};

const addVault = (vault) => {
  toPushVault.push(vault);
};

const addAccount = (vault) => {
  toPushAccounts.push(vault);
};

const tokenToPool = async (token) => {
  if (token.toString() in TOKENTOPOOLMAP) {
    return TOKENTOPOOLMAP[token.toString()];
  } else {
    log.debug("tokenToTool: " + token.toString());

    // this shit is now complicated, we need to find the mapping
    const pp = await btc("listpoolpairs");
    const keys = Object.keys(pp);
    keys.forEach((key) => {
      const pair = pp[key];
      if (
        pair.idTokenA == token.toString() ||
        pair.idTokenB == token.toString()
      ) {
        TOKENTOPOOLMAP[token.toString()] = key;
      }
    });

    log.debug("tokenToTool length: " + Object.keys(TOKENTOPOOLMAP).length);
    log.debug(TOKENTOPOOLMAP);
    if (token.toString() in TOKENTOPOOLMAP) {
      return TOKENTOPOOLMAP[token.toString()];
    } else {
      return undefined;
    }
  }
};

const retreiveAnyPoolOnDemand = async (poolId) => {
  if (poolId == undefined) resolve(undefined);
  if (!(poolId.toString() in MAPANYPOOL)) {
    // fetch ad
    var sort = [["blockHeight", -1.0]];
    var limit = 1;
    var cursor = dexprices.find({ poolId: poolId }).sort(sort).limit(limit);
    await cursor.forEach((doc) => {
      MAPANYPOOL[poolId.toString()] = doc;
    });
  }

  //console.log("retreived pool " + poolId);
  //console.log(MAPANYPOOL[poolId.toString()]);

  if (poolId.toString() in MAPANYPOOL) return MAPANYPOOL[poolId.toString()];
  else return undefined;
};
// must be called before any transaction is added AND at the beginning of each 400 block batch
const preFillMainPoolsFromDB = () => {
  return new Promise(async (resolve, reject) => {
    DFIUSDT = undefined;
    DUSDUSDT = undefined;
    DUSDDFI = undefined;

    var sort = [["blockHeight", -1.0]];
    var limit = 1;

    try {
      if (DFIUSDT == undefined) {
        var cursor = dexprices.find({ poolId: 6 }).sort(sort).limit(limit);
        await cursor.forEach((doc) => {
          DFIUSDT = doc;
        });
      }

      if (DUSDUSDT == undefined) {
        var cursor2 = dexprices.find({ poolId: 101 }).sort(sort).limit(limit);
        await cursor2.forEach((doc) => {
          DUSDUSDT = doc;
        });
      }

      if (DUSDDFI == undefined) {
        var cursor3 = dexprices.find({ poolId: 17 }).sort(sort).limit(limit);
        await cursor3.forEach((doc) => {
          DUSDDFI = doc;
        });
      }
    } catch (e) {
      reject(e);
      return;
    }

    resolve();
  });
};

// this must be called at the end of the tx processing
const consolidateDexPrices = () => {
  finalToPushDexPrices = [];
  Object.keys(toPushDexPrices).forEach((key) => {
    finalToPushDexPrices.push(toPushDexPrices[key]);
  });
};

// useless proxy function for now
const addSpecialTx = (tx, blockHash, blockHeight) => {
  return addTx(tx, blockHash, blockHeight);
};

const addTx = async (tx, blockHash, blockHeight) => {
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

  // record dex price if PoolSwap transaction
  if ("customTx" in tx && tx.customTx.type == "PoolSwap") {
    tx.state.reserve_changes.forEach((elem) => {
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

      const keyy = poolId.toString() + "$" + blockHeight.toString();
      if (keyy in toPushDexPrices) {
        const old_volumina = toPushDexPrices[keyy];
        obj.volume_a += old_volumina.volume_a;
        obj.volume_b += old_volumina.volume_b;
        delete toPushDexPrices[keyy];
      }

      toPushDexPrices[keyy] = obj;

      // always keep these prices up to date, to attach value to our trades or all transactions in general
      if (poolId == 6) DFIUSDT = toPushDexPrices[keyy]; // DFI-USDT POOL
      else if (poolId == 101)
        DUSDUSDT = toPushDexPrices[keyy]; // DUSD-USDT POOL
      else if (poolId == 17) DUSDDFI = toPushDexPrices[keyy]; // DUSD-DFI POOL
      else {
        MAPANYPOOL[poolId.toString()] = toPushDexPrices[keyy];
      }
    });
  }

  // now, all customTX receive a DEX price for the three main pools recorded
  if (!("state" in tx)) {
    tx.state = {};
  }

  if (!("main_pools" in tx.state)) {
    tx.state.main_pools = [];
  }
  if (!("balance_changes" in tx.state)) {
    tx.state.balance_changes = [];
  }
  if (!("reserve_changes" in tx.state)) {
    tx.state.reserve_changes = [];
  }

  if (DFIUSDT != undefined) tx.state.main_pools.push(DFIUSDT);
  if (DUSDDFI != undefined) tx.state.main_pools.push(DUSDDFI);
  if (DUSDUSDT != undefined) tx.state.main_pools.push(DUSDUSDT);

  // now add all other pools that are required ("balance changes in state")
  let doubles = [];
  for (let i = 0; i < tx.state.balance_changes.length; ++i) {
    const e = tx.state.balance_changes[i];
    // these are covered by the base token
    if (
      e.token.toString() == "0" ||
      e.token.toString() == "3" ||
      e.token.toString() == "13" ||
      e.token.toString() == "15"
    )
      return;

    const ttp = await tokenToPool(e.token);
    if (ttp != undefined) {
      const ret = await retreiveAnyPoolOnDemand(ttp);
      if (ret != undefined)
        if (!(ttp in doubles)) {
          doubles.push(ttp);
          tx.state.main_pools.push(ret);
        }
    }
  }

  toPushTxn.push(tx);
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

const commitTransaction = async (blockHeight) => {
  const query = { _id: crypto.createHash("md5").update("stats").digest("hex") };

  // make sure to trigger the Dex Price "fixing" - eg, write aggregated (by poolId) dex price and volumina per block
  consolidateDexPrices();

  await stats.replaceOne(query, cachedLastStatsUncomitted, {
    session: session,
    upsert: true,
  });
  log.info(
    "#" + blockHeight.toString() + ":",
    toPushBlocks.length,
    "blk",
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
  addSpecialTx,
  addBlock,
  addChainLastStats,
  shutup,
  addAccount,
  addVault,
  preFillMainPoolsFromDB,
};
