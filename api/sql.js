'use strict';

var express = require('express');
var mysqlx = require('@mysql/xdevapi');
const async = require('async');


async function getMyDb() {
  var mySession = await mysqlx.getSession( {
  host: process.env.MYSQL_HOST, port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER, password: process.env.MYSQL_PASSWORD} );

  await mySession.sql(`USE `+process.env.MYSQL_DATABASE).execute();
  return mySession;
}

function bin2hex(bin) {
  return Buffer.from(bin).toString('hex');
}


async function insertECRecord (astid, equid, assv) {
  var myDb = await getMyDb();
  if (assv == null) assv = 1;
  try {
    var sql = (equid == null) ? 
      'INSERT INTO EC SET astid = 0x'+astid+', assv = '+assv
      :
      'INSERT INTO EC SET equid = '+equid+', astid = 0x'+astid+', assv = '+assv;
    var result = await myDb.sql(sql).execute();
    if (result.getAffectedItemsCount()) {
      if (equid == null) equid = result.getAutoIncrementValue();
      console.log("EC record stored in SQL for equid/astid: " + equid + "/" + astid);
      return equid;
    }
  } catch (err) {
    if ('info' in err && 'code' in err.info && err.info.code === 1062) {
      console.log("EC record already exists: " +equid + "/" + astid);
      return equid;
    }
    console.error(err);
    return null;
  } finally {
    myDb.close();
  }
  return null;
}

// TODO: add fields type argn fnmod fntype fnclas

async function getRandomECAstId (astid) {
  var myDb = await getMyDb();
  try {
    // get a random equid for this astid
    var query =
      'select equid ' +
      'from EC ' +
      'where astid=0x' + astid + ' ' +
      'order by rand() ' +
      'limit 1';
      
    var res = await myDb.sql(query).execute();
    var recRaw = res.fetchOne();
    if (recRaw == null) {
      console.log('NO EQUIDS FOUND FOR ASTID '+astid)
      return null;
    }
    var equid = recRaw[0];
    // get the associative values (counts) for all the linked astid's for this astid
    var result = await myDb.sql(
      'select astid, assv ' +
      'from EC ' +
      'where equid = ' + equid + ' ' +
      'and astid != 0x' + astid
    ).execute();

    // calculate cumulative probabilities
    var rows = result.fetchAll();
    if (rows == null || rows.length == 0) {
      return null;
    }
    console.log(rows.length+' ASTIDS FOR EQUID')
    var cumulativeRows = [];
    var cum = 0;
    for (var i=0; i < rows.length; i++) {
      var row = rows[i];
      cum += row[1]; // add assv to cum
      cumulativeRows.push([ row[0], cum ]);
    }

    // get a random one by probability
    var ran = Math.random();
    var max = cumulativeRows[cumulativeRows.length-1][1];
    console.log('MAX: '+max);
    var recordRaw = null;

    for(var i = 0; i< cumulativeRows.length; i++) {
      recordRaw = cumulativeRows[i];
      if (recordRaw[1] / max <= ran) {
        break;
      }
    }

    if (recordRaw) {
      return bin2hex(recordRaw[0]); //astid
    }
  } catch (err) {
    console.error(err);
    return null;
  } finally {
    myDb.close();
  }
  return null;
}

async function getRandomByWordFreqency (astid) {
  var myDb = await getMyDb();
  try {
    var count = await myDb.sql(
        'select COUNT(*) from WordFreq'
    ).execute();

    var limit = count / 10;
    var random = getRandomInt(0, limit);

    // get a random equid for this astid
    var res = await myDb.sql(
        'select astid ' +
        'from WordFreq ' +
        'order by assv ' +
        'limit ' + random + ' ' +
        'offset ' + random
    ).execute();
    var recRaw = res.fetchOne();
    if (recRaw == null) {
      console.error('NO ASTID FOUND FOR RANDOM OFFSET: ' + random)
      return null;
    }
    var astid = recRaw[0];
  } catch (err) {
    console.error(err);
    return null;
  } finally {
    myDb.close();
  }
  return null;
}


async function getAssociativeValue(astid1, astid2) {
  var myDb = await getMyDb();
  try {
    var result = await myDb.sql('SELECT equid FROM EC WHERE astid = 0x'+astid1+' LIMIT 1').execute();
    var recordRaw = result.fetchOne();
    if (recordRaw) {
      var equid = recordRaw[0];
      var result2 = await myDb.sql('SELECT assv FROM EC WHERE equid='+equid+' AND astid=0x'+astid2+' LIMIT 1').execute();
      var recordRaw2 = result2.fetchOne();
      if (recordRaw2) {
        var assv = recordRaw2[0]
        return assv;
      }
    }
  } catch (err) {
    console.error(err);
    return null;
  } finally {
    myDb.close();
  }
  return null;
}

async function updateECRecord(equid, astid, assv) {
  var myDb = await getMyDb();
  try {
    var result = await myDb.sql('UPDATE EC SET assv = '+assv+' WHERE equid = '+equid+' AND astid = 0x'+astid).execute();
    if (result.getAffectedItemsCount()) {
      console.log("EC record updated in SQL for equid/astid: " + equid + "/" + astid);
      return true;
    }
  } catch (err) {
    console.error(err);
    return false;
  } finally {
    myDb.close();
  }
  return false;
}


// TODO when an astid is found with rows of differing equids, merge the two equivalence classes together to use one equid
async function incrementECRecord(astid1, astid2) {
  var myDb = await getMyDb();
  try {
    // find if equivalence class exists for either one of these
    var equid = null;
    var result1 = await myDb.sql('SELECT equid, astid FROM EC WHERE astid = 0x'+astid1).execute();
    var rows1 = result1.fetchAll();
    var result2 = await myDb.sql('SELECT equid, astid FROM EC WHERE astid = 0x'+astid2).execute();
    var rows2 = result2.fetchAll();

    // more than one EC row found
    var rows = rows1.concat(rows2);
    if (rows != null && rows.length > 1) {
      // there is more than one row
      // so there might be multiple equids
      // since equid+astid are unique sql column pairs
      // this means there is definately more than one equid (equivalence class)
      // if there are more than two rows
      // but since either astid is found in all of these they are actually the same
      // equivalence class
      // this is where they get merged, if needed
      var equidToMergeInto;
      var highestCount = 0;
      // get a list of all the unique equids along with their occurance count
      var equidsWithCounts = rows.reduce((total, row) => {
        let thisEquid = row[0];
        let thisCountSoFar;
        if (!total.includes(thisEquid)) {
          total.push([thisEquid,1]);
          thisCountSoFar = 1;
        } else {
          let thisEquidIndex = total.indexOf(thisEquid);
          thisCountSoFar = total[thisEquidIndex][1] + 1;
          total[thisEquidIndex][1] = thisCountSoFar;
        }
        if (thisCountSoFar > highestCount) {
          highestCount = thisCountSoFar;
          equidToMergeInto = thisEquid;
        }
        return total;
      }, []);
      // now get a simple list of all the equids that are not the one we are going to merge into
      var equidsToMerge = equidsWithCounts.reduce((total, entry) => {
        let thisEquid = entry[0];
        let thisCount = entry[1];
        if (thisCount < highestCount) total.push(thisEquid);
        return total;
      }, []);
      // actually perform the merge by changing all the other equids to the equidToMergeInto
      if (equidsToMerge.length > 0) {
        var equidsToMergeString = equidsToMerge.reduce((string, entry, index) => {
          string += entry;
          if (index+1 < equidsToMerge.length) string += ', ';
          return string;
        }, '(');
        equidsToMergeString += ')';
        var res2 = await myDb.sql('UPDATE EC SET equid = '+equidToMergeInto+' WHERE equid IN '+equidsToMergeString).execute();
        if (res2.getAffectedItemsCount() >= 1) {
          throw new Error("Failed to merge EC equid "+equidToMergeInto+" for "+equidsToMergeString);
        }
        console.log("EC records merged in SQL to equid " + equidToMergeInto + " for " + equidsToMergeString);
      }
      // ensure records for both astids exist
      var equidApplied = await ensureBothRecordsExist(equidToMergeInto, astid1, astid2);
      if (equidApplied == null) return false;
      // now have many rows for same equid, this is the fixed state
      // actually increment the existing record
      var incremented = await auxIncrementECRecord(equidToMergeInto, astid2);
      if (!incremented) return false;
      return equidToMergeInto;
    }

    // there is only one row, or no rows

    // select one of these to be the final equid
    if (rows1 != null && rows1.length > 0) equid = rows1[0][0];
    else if (rows2 != null && rows2.length > 0) equid = rows2[0][0];
    // equid might still be null, thats fine one will be generated
    
    // ensure records for both astids exist
    equid = await ensureBothRecordsExist(equid, astid1, astid2);
    if (equid == null) return false;
    // actually increment the existing record
    var incremented = await auxIncrementECRecord(equid, astid2);
    if (!incremented) return false;
    return equid;
  } catch (err) {
    console.error(err);
    return false;
  } finally {
    myDb.close();
  }
}


// make zero, one, or two records using the same equid
// if equid is null it will be generated
async function ensureBothRecordsExist(equid, astid1, astid2) {
  // equid is pass by value, dont need to worry about changing it here
  try {
    equid = await insertECRecord(astid1, equid);
    if (equid == null) {
      throw new Error("Failed to ensure row exists for existing EC: "+equid+" and astid1 "+astid1);
    }
    console.log("Created EC "+equid+" for "+astid1);

    equid = await insertECRecord(astid2, equid);
    if (equid == null) {
      throw new Error("Failed to ensure row exists for existing EC: "+equid+" and astid2 "+astid2);
    }
    console.log("Created EC "+equid+" for "+astid2);
  } catch (err) {
    console.error(err);
    return null;
  }
  return equid;
}

async function auxIncrementECRecord(equid, astid) {
  var myDb = await getMyDb();
  try {
    var result = await myDb.sql('UPDATE EC SET assv = assv + 1 WHERE equid = '+equid+' AND astid = 0x'+astid).execute();
    if (result.getAffectedItemsCount() >= 1) {
      console.log("EC record incremented in SQL for equid/astid: " + equid + "/" + astid);
      return true;
    }
    throw new Error("Failed to increment existing EC: "+equid+" and astid "+astid);
    console.error(JSON.stringify(result,null,4));
  } catch (err) {
    console.error(err);
    return false;
  } finally {
    myDb.close();
  }
  return false;
}

async function deleteECRecord(equid, astid) {
  var myDb = await getMyDb();
  try {
    var result = await myDb.sql('DELETE FROM EC WHERE equid = '+equid+' AND astid = 0x'+astid).execute();
    if (result.getAffectedItemsCount()) {
      console.log("EC record deleted in SQL for equid/astid: " + equid + "/" + astid);
      return true;
    }
  } catch (err) {
    console.error(err);
    return false;
  } finally {
    myDb.close();
  }
  return true; // already gone, so .. success (idempotent)
}

function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

module.exports = {
  bin2hex: bin2hex,
  insertECRecord: insertECRecord,
  getRandomECAstId: getRandomECAstId,
  getAssociativeValue: getAssociativeValue,
  updateECRecord: updateECRecord,
  incrementECRecord: incrementECRecord,
  deleteECRecord: deleteECRecord
};