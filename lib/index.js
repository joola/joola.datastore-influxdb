var
  influx = require('influx'),
  async = require('async'),
  util = require('util'),
  _ = require('underscore');

module.exports = InfluxDBProvider;

function InfluxDBProvider(options, helpers, callback) {
  if (!(this instanceof InfluxDBProvider)) return new InfluxDBProvider(options);

  callback = callback || function () {
  };

  var self = this;

  this.name = 'influxDB';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;

  return this.init(options, function (err) {
    if (err)
      return callback(err);

    return callback(null, self);
  });
}

InfluxDBProvider.prototype.init = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Initializing connection to provider [' + self.name + '].');

  return self.openConnection(options, callback);
};

InfluxDBProvider.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Destroying connection to provider [' + self.name + '].');

  return callback(null);
};

InfluxDBProvider.prototype.find = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

InfluxDBProvider.prototype.delete = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

InfluxDBProvider.prototype.update = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

InfluxDBProvider.prototype.insert = function (collection, documents, options, callback) {
  callback = callback || function () {
  };

  var self = this;

  documents.forEach(function (doc, i) {
    documents[i] = self.transform(collection.storeKey, doc);
  });

  var _documents = documents.clone();

  self.client.writePoints(collection.storeKey, _documents, {}, function (err) {
    if (err)
      return callback(err);

    documents.forEach(function (doc) {
      doc.timestamp = new Date(doc.time);
      doc.saved = true;
      delete doc.time;
    });
    return callback(null, documents);
  });
};

InfluxDBProvider.prototype.buildQueryPlan = function (query, callback) {
  var self = this;
  var plan = {
    uid: self.common.uuid(),
    cost: 0,
    colQueries: {},
    query: query
  };
  var $match = {};
  var $project = {};
  var $group = {};
  var $limit;

  if (!query.dimensions)
    query.dimensions = [];
  if (!query.metrics)
    query.metrics = [];

  if (query.timeframe && !query.timeframe.hasOwnProperty('last_n_items')) {
    if (typeof query.timeframe.start === 'string')
      query.timeframe.start = new Date(query.timeframe.start);
    if (typeof query.timeframe.end === 'string')
      query.timeframe.end = new Date(query.timeframe.end);
    $match.time = {$gt: query.timeframe.start.toISOString().replace('T', ' ').replace('Z', ''), $lt: query.timeframe.end.toISOString().replace('T', ' ').replace('Z', '')};
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = query.timeframe.last_n_items;
  }

  if (query.filter) {
    query.filter.forEach(function (f) {
      //if (f[1] == 'eq')
      //  $match[f[0]] = f[2];
      //else {
        $match[f[0]] = {};
        $match[f[0]]['$' + f[1]] = f[2];
      //}
    });
  }

  switch (query.interval) {
    case 'timebucket.second':
      query.interval = 's';
      break;
    case 'timebucket.minute':
      query.interval = 'm';
      break;
    case 'timebucket.hour':
      query.interval = 'h';
      break;
    case 'timebucket.ddate':
      query.interval = 'd';
      break;
    case 'timebucket.week':
      query.interval = 'w';
      break;
    default:
      break;
  }

  query.dimensions.forEach(function (dimension) {
    switch (dimension.datatype) {
      case 'date':
        $project.time = 'time';
        $group.time = 'time(1' + query.interval + ')';
        query.timeSeries = true;
        break;
      case 'ip':
      case 'number':
      case 'string':
        $project[dimension.key] = (dimension.attribute || dimension.key);
        $group[dimension.key] = (dimension.attribute || dimension.key);
        break;
      case 'geo':
        break;
      default:
        return setImmediate(function () {
          return callback(new Error('Dimension [' + dimension.key + '] has unknown type of [' + dimension.datatype + ']'));
        });
    }
  });

  if (query.metrics.length === 0) {
    try {
      query.metrics.push({
        key: 'fake',
        dependsOn: 'fake',
        collection: query.collection.key || query.dimensions ? query.dimensions[0].collection : null
      });
    }
    catch (ex) {
      query.metrics = [];
    }
  }

  query.metrics.forEach(function (metric) {
    var colQuery = {
      collections: metric.collection ? metric.collection.storeKey : null,
      query: []
    };

    var _$match = self.common.extend({}, $match);
    var _$project = self.common.extend({}, $project);
    var _$group = self.common.extend({}, $group);

    if (metric.filter) {
      metric.filter.forEach(function (f) {
        // if (f[1] === 'eq')
        //   _$match[f[0]] = f[2];
        //else {
        _$match[f[0]] = {};
        _$match[f[0]]['$' + f[1]] = f[2];
        // }
      });
    }
    if (!metric.formula && metric.collection) {
      metric.aggregation = metric.aggregation || 'sum';
      if (metric.aggregation == 'ucount')
        metric.aggregation = 'count(distinct';
      else if (metric.aggregation === 'avg')
        metric.aggregation = 'mean';

      colQuery.key = self.common.hash(colQuery.type + '_' + metric.collection.key + '_' + JSON.stringify(_$match));
      if (plan.colQueries[colQuery.key]) {
        _$group = self.common.extend({}, plan.colQueries[colQuery.key].query.$group);
        _$project = self.common.extend(plan.colQueries[colQuery.key].query.$project, _$project);
      }

      if (metric.key !== 'fake')
        _$project[metric.key] = metric.aggregation + '(' + (metric.attribute || metric.key) + ') ' + (metric.aggregation === 'count(distinct' ? ')' : '') + ' as ' + metric.key;

      colQuery.query = {
        $match: _$match,
        $sort: {time: -1},
        $project: _$project,
        $group: _$group
      };
      if ($limit) {
        colQuery.query.$limit = $limit;
      }

      plan.colQueries[colQuery.key] = colQuery;
    }

  });

  //console.log('plan', require('util').inspect(plan.colQueries, {depth: null, colors: true}));

  plan.dimensions = query.dimensions;
  plan.metrics = query.metrics;

  //console.log(plan);

  return setImmediate(function () {
    return callback(null, plan);
  });
};

InfluxDBProvider.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };

  var self = this;

  return self.buildQueryPlan(query, function (err, plan) {
    if (err)
      return callback(err);
    //console.log(require('util').inspect(plan.colQueries, {depth: null, colors: true}));

    var calls = [];
    var results = [];
    Object.keys(plan.colQueries).forEach(function (key) {
      var queryPlan = plan.colQueries[key].query;
      var queryPlanKey = key;
      var sql = 'select ';

      if (!queryPlan.$limit) {
        Object.keys(queryPlan.$project).forEach(function (key) {
          var column = queryPlan.$project[key];
          sql += column + ',';
        });
      }
      else {
        Object.keys(queryPlan.$project).forEach(function (key) {
          var column = queryPlan.$project[key];
          sql += 'top(' + (column === 'time' ? '_t' : column) + ',' + queryPlan.$limit + ') as ' + column + ',';
        });
      }
      if (sql.substring(sql.length - 1) === ',')
        sql = sql.substring(0, sql.length - 1);

      sql += ' from ' + plan.colQueries[key].collections;

      if (!queryPlan.$limit) {
        if (Object.keys(queryPlan.$group).length > 0) {
          sql += ' group by ';
          Object.keys(queryPlan.$group).forEach(function (key) {
            var column = queryPlan.$group[key];
            sql += column + ',';
          });
          if (sql.substring(sql.length - 1) === ',')
            sql = sql.substring(0, sql.length - 1);
          //if (query.timeSeries)
          // sql += ' fill(0)';
        }
      }

      if (Object.keys(queryPlan.$match).length > 0) {
        sql += ' where ';
        Object.keys(queryPlan.$match).forEach(function (key) {
          var filter = queryPlan.$match[key];
          var filterName = key;
          Object.keys(filter).forEach(function (key) {
            var f = filter[key];
            sql += filterName + ' ';
            switch (key) {
              case '$gt':
                sql += '> ';
                break;
              case '$gte':
                sql += '>= ';
                break;
              case '$lt':
                sql += '< ';
                break;
              case '$lte':
                sql += '<= ';
                break;
              default:
                sql += '= ';
                break;
            }
            switch (typeof f) {
              case 'number':
                sql += f + ' and ';
                break;
              case 'string':
              default:
                sql += '\'' + f + '\' and ';
                break;
            }
          });
        });
        if (sql.substring(sql.length - 4) === 'and ')
          sql = sql.substring(0, sql.length - 4);
      }
      //if (queryPlan.$limit && queryPlan.$limit > 0)
      //  sql += ' limit ' + queryPlan.$limit;

      //console.log(sql);

      var call = function (callback) {
        self.client.query(sql, function (err, result) {
          if (err)
            return callback(err);

          results.push(result);
          return callback(null);
        });
      };
      calls.push(call);
    });

    async.parallel(calls, function (err) {
      if (err)
        return callback(err);
      var output = {
        dimensions: query.dimensions,
        metrics: query.metrics,
        documents: [],
        queryplan: plan
      };

      var keys = [];
      var final = [];

      if (results && results.length > 0) {
        results.forEach(function (_result) {
          _result = _result[0];
          _result = self.verifyResult(query, _result);
          if (!_result)
            return callback(null, output);
          if (!_result.points)
            _result.points = [];

          var timeIndex = _result.columns.lastIndexOf('time');
          if (timeIndex > -1)
            _result.columns[timeIndex] = 'timestamp';

          _result.points.forEach(function (point) {
            var document = {_id: {}};
            _result.columns.forEach(function (col, i) {
              var dimension = _.find(query.dimensions, function (d) {
                return d.key === col.replace('.', '_');
              });
              if (dimension)
                document._id[col] = point[i];
              document[col] = point[i];
            });

            if (typeof document.timestamp === 'string')
              document.timestamp = parseInt(document.timestamp);
            if (document.timestamp > 0)
              document.timestamp = new Date(document.timestamp);

            var key = self.common.hash(JSON.stringify(document._id));
            var row;

            if (keys.indexOf(key) === -1) {
              row = {};
              Object.keys(document._id).forEach(function (key) {
                row[key] = document._id[key];
              });
              row.key = key;
              keys.push(key);
              final.push(row);
            }
            else {
              row = _.find(final, function (f) {
                return f.key == key;
              });
              if (!row) //TODO: how can this happen?
              {
                console.log('wtf?', key, query);
              }
            }

            Object.keys(document).forEach(function (attribute) {
              if (attribute != '_id') {
                if (document.hasOwnProperty(attribute)) {
                  if (attribute.indexOf('.') > -1) {
                    row[attribute.replace('.', '_')] = document[attribute];
                    delete row[attribute];
                  }
                  else
                    row[attribute] = document[attribute];
                }
                else
                  row[attribute] = '(not set)';
              }
            });
            output.metrics.forEach(function (m) {
              if (!row[m.key])
                row[m.key] = null;
            });

            //delete row.key;
            final[keys.indexOf(key)] = row;
          });
        });

        output.documents = final;

        //console.log(final);

        return setImmediate(function () {
          return callback(null, output);
        });
      }
      else {
        output.dimensions = plan.dimensions;

        output.metrics = plan.metrics;
        output.documents = [];
        return setImmediate(function () {
          return callback(null, output);
        });
      }
    });
  });
};

InfluxDBProvider.prototype.verifyResult = function (query, result) {
  //console.log(result);

  return result;
};

InfluxDBProvider.prototype.openConnection = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.client = influx(options);
  return callback(null, self.client);
};

InfluxDBProvider.prototype.closeConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

InfluxDBProvider.prototype.checkConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;


  return callback(null, connection);
};

InfluxDBProvider.prototype.transform = function (collection, obj) {
  var self = this;
  var result = {};
  obj.time = obj.time || new Date(obj.timestamp).getTime() || new Date().getTime();
  delete obj.timestamp;

  obj.f = 1;
  obj.t = obj.time;
  obj._t = obj.time.toString();

  var flat = self.common.flatten(obj);
  flat.forEach(function (pair) {
    result[pair[0]] = pair[1];
  });
  obj = result;

  return result;
};

InfluxDBProvider.prototype.stats = function (collection, callback) {
  var self = this;

  self.client.query('select count(f) from ' + collection, function (err, result) {
    if (err)
      return callback(err);

    return callback(null, {count: result});
  });
};

InfluxDBProvider.prototype.drop = function (collection, callback) {
  var self = this;

  self.client.query('drop series ' + collection, function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};


InfluxDBProvider.prototype.purge = function (callback) {
  var self = this;

  self.client.deleteDatabase('joola', function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};