//
// 97safestorage.js
// Date: 21.11.2017
// (c) Diogo Couto
//

var SSDB = alasql.engines.SAFESTORAGE = function (){};
var computedOutside;
var secure;
var password = 'supersecretpassword'
var key = password
var db

// var host = 'http://localhost:8080'
var host = 'http://192.168.112.54:8080/safe-storage-1.0';
const ROWS_PER_REQUEST = 50000;

SSDB.attachDatabase = function(ssdbid, dbid, args, params, cb) { 
  db = new alasql.Database(dbid || ssdbid); 
  db.engineid = "SAFESTORAGE";
  db.computedOutside = false
  db.secure = false
  db.ssdbid = ssdbid;
  db.tables = [];
  computedOutside = db.computedOutside;
  secure = db.secure;
  if(cb) cb(1)
}

SSDB.showDatabases = function(like,cb) {  
  return fetch(host + '/databases', {method: 'GET'})
  .then(function(response) {
    return response.json()
  })
  .then(function(response) {
    if(cb) cb(Object.keys(response.content))
  })
  .catch(function(error) {
    throw new Error(error.statusText)
  })
}

SSDB.createDatabase = function(ssdbid, args, ifnotexists, dbid, cb){ 
  var data = {
    'database_id': ssdbid,
    'if_exists' : ! ifnotexists
  }
  return fetch(host + '/databases/create', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if(cb) cb(1)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}


SSDB.dropDatabase = function(ssdbid, ifexists, cb){  
  var data = {
    'database_id': ssdbid,
    'if_exists' : ifexists
  }
  return fetch(host + '/databases/' + ssdbid + '/delete', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if(cb) cb(1)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.createTable = function(databaseid, tableid, ifnotexists, cb, columnsmap) {
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'if_exists' : ! ifnotexists,
    'fields': columnsmap
  }
  return fetch(host + '/databases/' + databaseid + '/tables/create', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if(cb) cb(1)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.dropTable = function (databaseid, tableid, ifexists, cb) {  
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'if_exists' : ! ifexists,
  }
  return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/delete', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if(cb) cb(1)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}


SSDB.intoTable = function(databaseid, tableid, values, columns, cb) {
  var requestData
  var encryptedColumns = db.tables[tableid].ecolumns

  if (secure) {
    requestData = encrypt(values, encryptedColumns)
  } else {
    requestData = values
  }

  // Check the number of rows is bigger than 200 000
  var size = values.length;
  var columnsNames = Object.keys(values[0])

  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'data' : requestData,
    'columns' : columnsNames,
    'fields': columns
  }

  var stringBuilder = ''
  stringBuilder += columnsNames.join(',') + '\n'

  for(var i = 0; i < size; i++) {
    stringBuilder += Object.values(requestData[i]).join(',') + '\n'
  }

  var blob = new Blob([stringBuilder], {type: 'text/plain'})
  var headers = {
    'Content-Type': 'text/plain'
  }

  return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/data', {method: 'POST',  headers: headers, body: blob})
  .then(function() {
    if(cb) cb(size)      
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
};


SSDB.fromTable = function(databaseid, tableid, cb, idx, query, whereStatement, orderByStatement, aliases, group){
  var data = {
    'table_id': tableid,    
    'database_id': databaseid,
    'fields': query.selectColumns,
    'aggregators': query.selectGroup,
    'distinct': query.distinct,
    'limit': query.top ? query.top : (query.limit ? query.limit : -1),
    'percentage': query.percent ? query.percent : 100,
    'offset': query.offset ? query.offset : 0,    
    'where' : whereStatement ? whereStatement.expression : undefined,
    'order_by' : orderByStatement ? orderByStatement : undefined,
    'aliases': query.aliases,
    'columns' : query.xcolumns,
    'column_aliases' : aliases,
    'group_by': group,
  }
  var encryptedColumns = db.tables[tableid].ecolumns  
  if(computedOutside) {
    query.distinct = false
    query.selectColumns = {}
    return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/where', {method: 'POST', body: JSON.stringify(data)})
    .then(function(response) {
      return response.json()
    })
    .then(function(response) {
      var result
      if(secure) {
        result = decrypt(response.content.data, encryptedColumns)
      } else {
        result = response.content.data
      }
      query.join = result
      if(cb) cb(result, idx, query)
    })
    .catch(function(error) {
      if(cb) cb(0, idx, query)
    })
  } else {
    return fetch(host + '/databases/' + databaseid + '/tables/' + tableid, {method: 'POST', body: JSON.stringify(data)})
    .then(function(response) {
      return response.json()
    })
    .then(function(response) {
      var result
      if(secure) {
        result = decrypt(response.content.data, encryptedColumns)
      } else {
        result = response.content.data
      }
      if(cb) cb(result, idx, query)
    })
    .catch(function(error) {
      if(cb) cb(0, idx, query)
    })
  }
}

SSDB.joinTable = function(databaseid, tableid, cb, idx, query, whereStatement, orderByStatement, joinStatement, aliases, group){
  var data = {
    'database_id': databaseid,
    'fields': query.selectColumns,
    'aggregators': query.selectGroup,
    'distinct': query.distinct,
    'limit': query.top ? query.top : (query.limit ? query.limit : -1),
    'percentage': query.percent ? query.percent : 100,
    'offset': query.offset ? query.offset : 0,    
    'where' : whereStatement ? whereStatement.expression : undefined,
    'order_by' : orderByStatement ? orderByStatement : undefined,
    'join_statement': joinStatement,
    'aliases': query.aliases,
    'columns' : query.xcolumns,
    'column_aliases' : aliases,
    'group_by': group,
  }
  query.distinct = false
  query.selectColumns = {}
  return fetch(host + '/databases/' + databaseid + '/join', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    return response.json()
  })
  .then(function(response) {
    var result
    if(secure) {
      result = decrypt(response.content.data, encryptedColumns)
    } else {
      result = response.content.data
    }
    query.join = result
    if(cb) cb(result, idx, query)
  })
  .catch(function(error) {
    if(cb) cb(0, idx, query)
  })
}

SSDB.deleteFromTable = function(databaseid, tableid, wherefn, params, cb, whereStatement){
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'where': whereStatement
  }
  return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/data/delete', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    return response.json()
  })
  .then(function(response) {
    if(cb) cb(response.content.length)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.truncateTable = function(databaseid,tableid, ifexists, cb){
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'if_exists' : ! ifexists,    
  }
  return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/truncate', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    return response.json()
  })
  .then(function(response) {
    if(cb) cb(response.content.length)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.alterTable = function(databaseid, tableid, cb, alteration) {
  data = Object.assign(alteration, {
      'database_id': databaseid,
      'table_id': tableid,
  });
  return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/alter', {method: 'PUT', body: JSON.stringify(data)})
  .then(function(response) {
    if(cb) cb(response.data.content.length)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.updateTable = function(databaseid, tableid, assignfn, wherefn, params, cb, whereStatement, assignStatement){
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'assign': assignStatement,
    'where': whereStatement
  }
  return fetch(host + '/databases/' + databaseid + '/tables/' + tableid + '/data', {method: 'PUT', body: JSON.stringify(data)})
  .then(function(response) {
    return response.json()
  })
  .then(function(response) {
    if(cb) cb(response.content.length)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

function clone(obj) {
  if (null == obj || "object" != typeof obj) return obj;
  var copy = obj.constructor();
  for (var attr in obj) {
      if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
  }
  return copy;
}


function slice(obj, start, end) {
  var result = {};
  var i = 0;
  for (var key in obj) {
    if(i >= end) {
      break
    } else if (i >= start && obj.hasOwnProperty(key)) {
        result[key] =  obj[key];
    }
    i++
  }
  return result
}

function encrypt(values, encryptedColumns) {
  var requestData = [];
  for(var i = 0; i < values.length; i++) {
    var row = values[i];
    var encryptedRow = {}
    
    for(var column in row) {
      if(row.hasOwnProperty(column)) {
        if (encryptedColumns.includes(column)) {
          encryptedRow[column] = CryptoJS.AES.encrypt(row[column], key).toString()
          // encryptedRow[column] = CryptoJS.enc.Base64.stringify(CryptoJS.AES.encrypt(row[column], key).ciphertext)
        } else {
          encryptedRow[column] = row[column]
        }
      }
    }

    requestData.push(encryptedRow)
  }

  return requestData
}

function decrypt(values, encryptedColumns) {
  var requestData = [];
  for(var i = 0; i < values.length; i++) {
    var row = values[i];
    var decryptedRow = {}
    
    for(var column in row) {
      if(row.hasOwnProperty(column)) {
        if (encryptedColumns.includes(column)) {
          decryptedRow[column] = CryptoJS.AES.decrypt(row[column], key).toString(CryptoJS.enc.Utf8)
        } else {
          decryptedRow[column] = row[column]
        }
      }
    }

    requestData.push(decryptedRow)
  }

  return requestData
}
