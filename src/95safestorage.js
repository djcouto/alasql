//
// 97safestorage.js
// Date: 21.11.2017
// (c) Diogo Couto
//

var SSDB = alasql.engines.SAFESTORAGE = function (){};
var computedOutside = false;
var secure = true;
var password = 'supersecretpassword';
var key = password;
var db;

// var storageHost = 'http://localhost:8080'
var storageHost = 'http://192.168.112.54:8080/safe-storage-1.0';
// var encryptionHost = 'http://localhost:8089';
var encryptionHost = 'http://192.168.112.54:8080/crypto-server-1.0';

SSDB.attachDatabase = function(ssdbid, dbid, args, params, cb) { 
  db = new alasql.Database(dbid || ssdbid); 
  db.engineid = "SAFESTORAGE";
  db.computedOutside = computedOutside;
  db.secure = secure;
  db.ssdbid = ssdbid;
  db.tables = [];
  computedOutside = db.computedOutside;
  if(cb) cb(1)
}

SSDB.showDatabases = function(like,cb) {  
  return fetch(storageHost + '/databases', {method: 'GET'})
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
  return fetch(storageHost + '/databases/create', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if (secure) {
      return fetch(encryptionHost + '/databases/create', {method: 'POST', body: JSON.stringify(data)})
    } else {
      return Promise.resolve()
    }
  })
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
  return fetch(storageHost + '/databases/' + ssdbid + '/delete', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if (secure) {
      return fetch(encryptionHost + '/databases/' + ssdbid + '/delete', {method: 'POST', body: JSON.stringify(data)})
    } else {
      return Promise.resolve()
    }
  })
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
  return fetch(storageHost + '/databases/' + databaseid + '/tables/create', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if (secure) {
      return fetch(encryptionHost + '/databases/' + databaseid + '/tables/create', {method: 'POST', body: JSON.stringify(data)})
    } else {
      return Promise.resolve()
    }
  })
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
  return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/delete', {method: 'POST', body: JSON.stringify(data)})
  .then(function(response) {
    if (secure) {
      return fetch(encryptionHost + '/databases/' + databaseid + '/tables/' + tableid + '/delete', {method: 'POST', body: JSON.stringify(data)})
    } else {
      return Promise.resolve()
    }
  })
  .then(function(response) {
    if(cb) cb(1)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}


SSDB.intoTable = function(databaseid, tableid, values, columns, cb) {
  // Check the number of rows is bigger than 200 000
  var size = values.length;
  var columnsNames = Object.keys(values[0])

  var stringBuilder = ''
  stringBuilder += columnsNames.join(',')

  for(var i = 0; i < size; i++) {
    stringBuilder += '\n' + Object.values(values[i]).join(',')
  }

  var blob = new Blob([stringBuilder], {type: 'text/plain'})
  var headers = {
    'Content-Type': 'text/plain'
  }

  if(secure) {
    return fetch(encryptionHost + '/data/' + databaseid + '/tables/' + tableid + '/encrypt', {method: 'POST',  headers: headers, body: blob})
    .then(function() {
      if(cb) cb(size)      
    })
    .catch(function(error) {
      if(cb) cb(0)
    })
  } else {
    return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/data', {method: 'POST',  headers: headers, body: blob})
    .then(function() {
      if(cb) cb(size)      
    })
    .catch(function(error) {
      if(cb) cb(0)
    })
  }
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
    return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/where', {method: 'POST', body: JSON.stringify(data)})
    .then(function(response) {
      return response.json()
    })
    .then(function(response) {
      if(secure) {
        // As it was computed outside, we will only decrypt some specifics fields
        var hasColumnsToDecrypt = false
        var decryptedColumns = []
        var aliasesMap = {}
        var ob = {}
        var table = db.tables[tableid]

        for(var i = 0; i < aliases.length; i++) {
          var aliase = aliases[i]
          if(aliase.hasOwnProperty('as') && aliase.hasOwnProperty('columnid')) {
            aliasesMap[aliase['as']] = aliase['columnid']
          }
        }

        var stringBuilder = ''
        stringBuilder += 'techniques' + '\n'
        
        var data = response.content.data

        var columnsNames = Object.keys(data[0])

        for(var i = 0; i < columnsNames.length; i++) {
          var technique
          if(table.ecolumns.hasOwnProperty(columnsNames[i])) {
            hasColumnsToDecrypt = true
            technique = table.ecolumns[columnsNames[i]].encryption_technique
          } else if (aliasesMap.hasOwnProperty(columnsNames[i])) {
            var columnName = aliasesMap[columnsNames[i]]
            if(table.ecolumns.hasOwnProperty(columnName)) {
              hasColumnsToDecrypt = true              
              technique = table.ecolumns[columnName].encryption_technique
            } else {
              technique = 'PLAIN'
            }
          } else {
            technique = 'PLAIN'
          }

          if (i === columnsNames.length - 1) {
            stringBuilder += technique + '\n'
          } else {
            stringBuilder += technique + ','            
          }
        }

        if(hasColumnsToDecrypt) {
          stringBuilder += columnsNames.join(',')

          for(var i = 0; i < data.length; i++) {
            stringBuilder += '\n' + Object.values(data[i]).join(',')
          }
  
          var blob = new Blob([stringBuilder], {type: 'text/plain'})          
          var headers = {
            'Content-Type': 'text/plain'
          }
  
          // Decrypt the data
          return fetch(encryptionHost + '/data/' + databaseid + '/tables/' + tableid + '/decrypt', {method: 'POST',  headers: headers, body: blob})
          .then(function(response) {
            return response.json()
          })
          .then(function(response) {
            var result = response.content.data
            query.join = result
            if(cb) cb(result, idx, query)
          })
          .catch(function(error) {
            if(cb) cb(0, idx, query)
          })
        } else {
          var result = response.content.data
          query.join = result
          if(cb) cb(result, idx, query)
        }
      } else {
        var result = response.content.data
        query.join = result
        if(cb) cb(result, idx, query)
      }
    })
    .catch(function(error) {
      if(cb) cb(0, idx, query)
    })
  } else {
    return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid, {method: 'POST', body: JSON.stringify(data)})
    .then(function(response) {
      return response.json()
    })
    .then(function(response) {
      if(secure) {
        // We will need to decrypt all the tables to be able to execute the query        
        var data = response.content.data
        var size = data.length;
        
        var columnsNames = Object.keys(data[0])

        var stringBuilder = ''
        stringBuilder += 'columns' + '\n'
        stringBuilder += columnsNames.join(',')

        for(var i = 0; i < size; i++) {
          stringBuilder += '\n' + Object.values(data[i]).join(',')
        }

        var blob = new Blob([stringBuilder], {type: 'text/plain'})
        var headers = {
          'Content-Type': 'text/plain'
        }

        return fetch(encryptionHost + '/data/' + databaseid + '/tables/' + tableid + '/decrypt', {method: 'POST',  headers: headers, body: blob})
        .then(function(response) {
          return response.json()
        })
        .then(function(response) {
          if(cb) cb(response.content.data, idx, query)
        })
      } else {
        if(cb) cb(response.content.data, idx, query)
      }
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
  return fetch(storageHost + '/databases/' + databaseid + '/join', {method: 'POST', body: JSON.stringify(data)})
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
  return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/data/delete', {method: 'POST', body: JSON.stringify(data)})
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
  return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/truncate', {method: 'POST', body: JSON.stringify(data)})
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
  return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/alter', {method: 'PUT', body: JSON.stringify(data)})
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
  return fetch(storageHost + '/databases/' + databaseid + '/tables/' + tableid + '/data', {method: 'PUT', body: JSON.stringify(data)})
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
