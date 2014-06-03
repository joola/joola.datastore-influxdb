var
  influx = require('influx'),
  async = require('async'),
  moment = require('moment'),
  traverse = require('traverse');

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

  documents.forEach(function (doc) {
    self.transform(collection.storeKey, doc);
  });

  //console.log(require('util').inspect(documents, {depth: null, colors: true}));

  self.client.writePoints(collection.storeKey, documents, {}, function (err, result) {
    if (err)
      return callback(err);
    return callback(null, result);
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
    $match.time = {$gt: moment(query.timeframe.start).format('YYYY-MM-DD HH:mm:ss.SSS'), $lt: moment(query.timeframe.end).format('YYYY-MM-DD HH:mm:ss.SSS')};
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = {$limit: query.timeframe.last_n_items};
  }

  if (query.filter) {
    query.filter.forEach(function (f) {
      if (f[1] == 'eq')
        $match[f[0]] = f[2];
      else {
        $match[f[0]] = {};
        $match[f[0]]['$' + f[1]] = f[2];
      }
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
    case 'timebucket.day':
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

  query.metrics.forEach(function (metric) {
    var colQuery = {
      collections: metric.collection ? metric.collection.storeKey : null,
      query: []
    };

    if (!metric.formula && metric.collection) {
      metric.aggregation = metric.aggregation || 'sum';
      if (metric.aggregation == 'ucount')
        metric.aggregation = 'distinct';
      else if (metric.aggregation === 'avg')
        metric.aggregation = 'mean';

      var _$match = self.common.extend({}, $match);
      var _$project = self.common.extend({}, $project);
      var _$group = self.common.extend({}, $group);

      colQuery.key = self.common.hash(colQuery.type + '_' + metric.collection.key + '_' + JSON.stringify(_$match));
      if (plan.colQueries[colQuery.key]) {
        _$group = self.common.extend({}, plan.colQueries[colQuery.key].query.$group);
        _$project = self.common.extend(plan.colQueries[colQuery.key].query.$project, _$project);
      }

      _$project[metric.key] = metric.aggregation + '(' + (metric.attribute || metric.key) + ')';

      if (metric.filter) {
        metric.filter.forEach(function (f) {
          if (f[1] == 'eq')
            _$match[f[0]] = f[2];
          else {
            _$match[f[0]] = {};
            _$match[f[0]]['$' + f[1]] = f[2];
          }
        });
      }

      colQuery.query = {
        $match: _$match,
        $sort: {time: -1},
        $project: _$project,
        $group: _$group
      };
    }

    if ($limit) {
      colQuery.query.$limit = $limit;
    }

    plan.colQueries[colQuery.key] = colQuery;
  });

  //console.log(require('util').inspect(plan.colQueries, {depth: null, colors: true}));

  plan.dimensions = query.dimensions;
  plan.metrics = query.metrics;

  return setImmediate(function () {
    return callback(null, plan);
  });
};

InfluxDBProvider.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };

  var self = this;

  return self.buildQueryPlan(query, function (err, plan) {
    //console.log(require('util').inspect(plan.colQueries, {depth: null, colors: true}));

    var calls = [];
    var results = {};
    Object.keys(plan.colQueries).forEach(function (key) {
      var queryPlan = plan.colQueries[key].query;
      var queryPlanKey = key;
      var sql = 'select ';
      Object.keys(queryPlan.$project).forEach(function (key) {
        var column = queryPlan.$project[key];
        sql += column + ',';
      });
      if (sql.substring(sql.length - 1) === ',')
        sql = sql.substring(0, sql.length - 1);

      sql += ' from ' + plan.colQueries[key].collections;

      sql += ' group by ';
      Object.keys(queryPlan.$group).forEach(function (key) {
        var column = queryPlan.$group[key];
        sql += column + ',';
      });
      if (sql.substring(sql.length - 1) === ',')
        sql = sql.substring(0, sql.length - 1);
      if (Object.keys(queryPlan.$group).length > 0)
        sql += ' fill(0)';

      if (queryPlan.$match) {
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
          if (sql.substring(sql.length - 4) === 'and ')
            sql = sql.substring(0, sql.length - 4);
        });
      }
      //console.log(sql);

      var call = function (callback) {
        console.log(sql);
        self.client.query(sql, function (err, result) {
          if (err)
            return callback(err);

          results[queryPlanKey] = result;
          return callback(null);
        });
      };
      calls.push(call);
    });

    async.parallel(calls, function (err) {
      if (err)
        return callback(err);

      console.log(require('util').inspect(results, {depth: null, colors: true}));

      return callback(null, results);
    });
  });
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

  obj.time = obj.time || obj.timestamp || new Date();
  delete obj.timestamp;

  return obj;
};