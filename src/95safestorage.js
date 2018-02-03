//
// 97safestorage.js
// AlaSQL IndexedDB module
// Date: 21.11.2017
// (c) Diogo Couto
//

var SSDB = alasql.engines.SAFESTORAGE = function (){};
var computedOutside;

SSDB.attachDatabase = function(ssdbid, dbid, args, params, cb) { 
  var db = new alasql.Database(dbid || ssdbid);
  db.engineid = "SAFESTORAGE";
  db.computedOutside = true
  db.ssdbid = ssdbid;
  db.tables = [];
  computedOutside = db.computedOutside;
  if(cb) cb(1)
}

SSDB.showDatabases = function(like,cb) {  
  axios.get('http://localhost:4567/databases')
  .then(function(response) {
    if(cb) cb(Object.keys(response.data.content))
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
  axios.post('http://localhost:4567/databases/create', data)
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
  return axios.post('http://localhost:4567/databases/' + ssdbid + '/delete', data)
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
  return axios.post('http://localhost:4567/databases/' + databaseid + '/tables/create', data)
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
  return axios.post('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/delete', data)
  .then(function(response) {
    if(cb) cb(1)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.intoTable = function(databaseid, tableid, value, columns, cb) {  
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'data' : value,
  }
  return axios.post('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/data', data)
  .then(function(response) {
    if(cb) cb(value.length)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}


SSDB.fromTable = function(databaseid, tableid, cb, idx, query, whereStatement, orderByStatement){
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
    'fields': query.selectColumns,
    'distinct': query.distinct,
    'limit': query.top ? query.top : (query.limit ? query.limit : -1),
    'percentage': query.percent ? query.percent : 100,
    'offset': query.offset ? query.offset : 0,    
    'where' : whereStatement ? whereStatement.expression : undefined,
    'order_by' : orderByStatement ? orderByStatement : undefined
  }
  if(computedOutside) {
    query.distinct = false
    query.selectColumns = {}
    return axios.post('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/where', data)
    .then(function(response) {
      if(cb) cb(response.data.content.data, idx, query)
    })
    .catch(function(error) {
      if(cb) cb(0, idx, query)
    })
  } else {
    return axios.get('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid)
    .then(function(response) {
      if(cb) cb(response.data.content.data, idx, query)
    })
    .catch(function(error) {
      if(cb) cb(0, idx, query)
    })
  }
}

SSDB.deleteFromTable = function(databaseid, tableid, wherefn, params, cb, whereStatement){
  var data = {
    'database_id': databaseid,
    'table_id': tableid,
  }
  if(computedOutside) {
    data['where'] = whereStatement
  } else {
    data['data'] = params
  }
  return axios.post('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/data/delete', data)
  .then(function(response) {
    if(cb) cb(response.data.content.length)
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
  return axios.post('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/truncate', data)
  .then(function(response) {
    if(cb) cb(response.data.content.length)
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
  return axios.put('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/alter', data)
  .then(function(response) {
    if(cb) cb(response.data.content.length)
  })
  .catch(function(error) {
    if(cb) cb(0)
  })
}

SSDB.updateTable = function(databaseid, tableid, assignfn, wherefn, params, cb, whereStatement, assignStatment){
  var updated = []
  var data = {
    'database_id': databaseid,
    'table_id': tableid
  }
  if(computedOutside) {
    data['where'] = whereStatement
    data['assign'] = assignStatment
  } else {
    data['data'] = params
  }
  return axios.put('http://localhost:4567/databases/' + databaseid + '/tables/' + tableid + '/data', data)
  .then(function(response) {
    if(cb) cb(response.data.content.length)
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