import _ from 'lodash';
import Promise from 'bluebird';
import md5 from 'md5';
import performanceNow from 'performance-now';
import buildRedisClient from 'humane-node-commons/lib/RedisClient';
import * as Request from 'humane-node-commons/lib/Request';
import InternalServiceError from 'humane-node-commons/lib/InternalServiceError';

export default class ESClient {
    constructor(config) {
        this.request = Request.builder(_.extend({}, config.esConfig, {logLevel: config.logLevel, baseUrl: (config.esConfig && config.esConfig.url) || 'http://localhost:9200'}));

        this.redisKeyPrefix = process.env.REDIS_KEY_PREFIX;
        if (this.redisKeyPrefix) {
            this.redisKeyPrefix = `${this.redisKeyPrefix}/`;
        } else {
            this.redisKeyPrefix = '';
        }

        this.redisClient = buildRedisClient(_.pick(config, ['redisConfig', 'redisSentinelConfig']));
    }

    // throw new InternalServiceError('Internal Service Error', {code: 'INTERNAL_SERVICE_ERROR', details: response.body && response.body.error || response.body});
    storeInCache(key, data) {
        // nice to have: pack data with MessagePack
        return this.redisClient.setAsync([this.redisKeyPrefix + key, JSON.stringify(data), 'EX', 300])
          .then(() => data)
          .catch(() => {
              console.error('REDIS_ERROR: Error in storing key: ', this.redisKeyPrefix + key);
              return null;
          }); // eat the error
    }

    retrieveFromCache(key) {
        // nice to have: pack data with MessagePack
        return this.redisClient.getAsync(this.redisKeyPrefix + key)
          .then((data) => {
              if (!_.isUndefined(data) && !_.isNull(data) && _.isString(data)) {
                  return JSON.parse(data);
              }

              return null;
          })
          .catch(() => {
              console.error('REDIS_ERROR: Error in retrieving key: ', this.redisKeyPrefix + key);
              return null;
          }); // eat the error
    }

    removeFromCache(key) {
        return this.redisClient.delAsync(this.redisKeyPrefix + key)
          .catch(() => {
              console.error('REDIS_ERROR: Error in removing key: ', this.redisKeyPrefix + key);
              return null;
          }); // eat the error
    }

    static processResponse(response) {
        let _response = response;
        if (_.isArray(_response)) {
            _response = response[0];
        }

        if (_response.statusCode < 400) {
            return _response.body;
        }

        // console.error('Error: ', _response.body);

        throw new InternalServiceError('Internal Service Error', {_statusCode: _response.statusCode, details: (_response.body && _response.body.error) || _response.body});
    }

    // queries will be in following format:
    //      index or indices
    //      type
    //      search
    static bulkFormat(queries) {
        let ret = '';
        _.forEach(queries, (query) => {
            ret += JSON.stringify({index: (query.indices || [query.index]).join(','), type: (query.types || [query.type]).join(',')});
            ret += '\n';
            ret += JSON.stringify(query.search);
            ret += '\n';
        });

        return ret;
    }

    allPages(index, type, query, size, cb) {
        const _this = this;

        let totalResults = 0;

        function recursiveFetch(page) {
            return _this.search({index, type, search: _.assign({from: page * size, size}, query)})
              .then((response) => {
                  if (response && response.hits) {
                      if (page === 0) {
                          totalResults = response.hits.total;
                      }

                      // process response...
                      cb(response);

                      const hits = response.hits.hits;
                      if (hits && totalResults > (page * size) + hits.length) {
                          // recursively fetch next page
                          return recursiveFetch(page + 1);
                      }
                  }

                  return true;
              });
        }

        return recursiveFetch(0)
          .catch((error) => {
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    search(queryOrPromise) {
        const startTime = performanceNow();

        return Promise.resolve(queryOrPromise)
          .then((query) => {
              const uri = !query.type ? `/${query.index}/_search` : `/${query.index}/${query.type}/_search`;

              console.log('search: ', uri, JSON.stringify(query.search));

              const queryKey = md5(JSON.stringify(query.search));
              const cacheKey = `${uri}:${queryKey}`;

              return this.retrieveFromCache(cacheKey)
                .then((cacheResponse) => {
                    if (cacheResponse) {
                        cacheResponse.took = _.round(performanceNow() - startTime, 3);

                        console.log('search: Retrieved from cache in (ms): ', cacheResponse.took);

                        return cacheResponse;
                    }

                    return this.request({method: 'POST', uri, body: query.search})
                      .then(response => Request.handleResponse(response))
                      .then((queryResponse) => {
                          console.log('search: in (ms): ', _.round(performanceNow() - startTime, 3));
                          if (queryResponse) {
                              return this.storeInCache(cacheKey, queryResponse);
                          }

                          return null;
                      });
                });
          })
          .catch((error) => {
              console.error('Error: ', error, error.stack);
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    explain(id, query) {
        const uri = `/${query.index}/${query.type}/${id}/_explain`;

        //console.log('Explain: ', uri, JSON.stringify(query.search));

        return this.request({method: 'POST', uri, body: query.search})
          .then(response => Request.handleResponse(response))
          .catch((error) => {
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    get(index, type, id) {
        const startTime = performanceNow();

        const uri = `/${index}/${type}/${id}`;

        const cacheKey = md5(uri);

        return this.retrieveFromCache(cacheKey)
          .then((cacheResponse) => {
              if (cacheResponse) {
                  cacheResponse.took = _.round(performanceNow() - startTime, 3);

                  console.log('get: Retrieved from cache in (ms): ', cacheResponse.took);

                  return cacheResponse;
              }

              return this.request({method: 'GET', uri})
                .then(response => Request.handleResponse(response))
                .then((getResponse) => {
                    console.log('get: in (ms): ', _.round(performanceNow() - startTime, 3));
                    if (getResponse) {
                        return this.storeInCache(cacheKey, getResponse);
                    }

                    return null;
                });
          })
          .catch((error) => {
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    termVectors(index, type, id) {
        const uri = `/${index}/${type}/${id}/_termvectors?fields=*`;

        return this.request({method: 'GET', uri})
          .then(response => Request.handleResponse(response))
          .catch((error) => {
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    didYouMean(index, query) {
        const uri = `/${index}/_didYouMean?q=${query}`;

        return this.request({method: 'GET', uri})
          .then(response => Request.handleResponse(response))
          .catch((error) => {
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    intent(index, query) {
        const startTime = performanceNow();

        const uri = `/${index}/_intent`;

        const queryKey = md5(JSON.stringify(query));
        const cacheKey = `${uri}:${queryKey}`;

        return this.retrieveFromCache(cacheKey)
          .then((cacheResponse) => {
              if (cacheResponse) {
                  cacheResponse.took = _.round(performanceNow() - startTime, 3);

                  console.log('intent: Retrieved from cache in (ms): ', cacheResponse.took);

                  return cacheResponse;
              }

              console.log('intent: ', uri, JSON.stringify(query));

              return this.request({method: 'POST', uri, body: query})
                .then(response => Request.handleResponse(response))
                .then((queryResponse) => {
                    console.log('intent: in (ms): ', _.round(performanceNow() - startTime, 3));
                    if (queryResponse) {
                        return this.storeInCache(cacheKey, queryResponse);
                    }

                    return null;
                });
          })
          .catch((error) => {
              console.error('Error: ', error, error.stack);
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    multiSearch(queriesOrPromise) {
        const startTime = performanceNow();

        return Promise.all(queriesOrPromise)
          .then((queries) => {
              const uri = '/_msearch';

              console.log('multiSearch: ', JSON.stringify(queries));

              const bulkQuery = ESClient.bulkFormat(queries);

              const queryKey = md5(bulkQuery);
              const cacheKey = `${uri}:${queryKey}`;

              return this.retrieveFromCache(cacheKey)
                .then((cacheResponse) => {
                    if (cacheResponse) {
                        cacheResponse.took = _.round(performanceNow() - startTime, 3);

                        if (cacheResponse.responses) {
                            // set response times
                            _.forEach(cacheResponse.responses, (response) => {
                                response.took = cacheResponse.took;
                            });
                        }

                        console.log('multiSearch: Retrieved from cache in (ms): ', cacheResponse.took);
                        return cacheResponse;
                    }

                    return this.request({method: 'POST', uri, body: bulkQuery, json: false})
                      .then(ESClient.processResponse)
                      .then((response) => {
                          if (!_.isUndefined(response) && !_.isNull(response) && _.isString(response)) {
                              return JSON.parse(response);
                          }

                          return null;
                      })
                      .then((queryResponse) => {
                          console.log('multiSearch: in (ms): ', _.round(performanceNow() - startTime, 3));
                          if (queryResponse) {
                              return this.storeInCache(cacheKey, queryResponse);
                          }

                          return null;
                      });
                });
          })
          .catch((error) => {
              console.error('Error: ', error, error.stack);
              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
          });
    }

    // analyze(index, analyzer, text) {
    //     const uri = `/${index}/_analyze?analyzer=${analyzer}&text=${encodeURIComponent(text)}`;
    //     return this.request({method: 'GET', uri})
    //       .then(ESClient.processResponse)
    //       .catch(error => {
    //           throw new InternalServiceError('Internal Service Error', {details: error && error.cause || error, stack: error && error.stack});
    //       });
    // }

//    curl -XGET 'http://localhost:9200/imdb/movies/_validate/query?rewrite=true' -d '
//{
//    "query": {
//      "fuzzy": {
//        "actors": "kyle"
//        }
//      }
//}'
//
//{
//    "valid": true,
//    "_shards": {
//      "total": 1,
//      "successful": 1,
//      "failed": 0
//      },
//    "explanations": [
//    {
//        "index": "imdb",
//        "valid": true,
//        "explanation": "plot:kyle plot:kylie^0.75 plot:kyne^0.75 plot:lyle^0.75 plot:pyle^0.75 #_type:movies"
//    }
//      ]
//}
}