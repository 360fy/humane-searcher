// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import {EventEmitter} from 'events';
import ESClient from './ESClient';
import * as Constants from './Constants';
import buildApiSchema from './ApiSchemaBuilder';
import SearchEventHandler from './SearchEventHandler';
import LanguageDetector from 'humane-node-commons/lib/LanguageDetector';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import InternalServiceError from 'humane-node-commons/lib/InternalServiceError';

const langFilter = {
    field: '_lang',
    termQuery: true,
    value: (value) => {
        if (value.secondary) {
            return _.union([value.primary], value.secondary);
        }

        return value.primary;
    }
};

class SearcherInternal {
    constructor(config) {
        this.logLevel = config.logLevel || 'info';
        this.instanceName = config.instanceName;

        const DefaultTypes = {
            searchQuery: {
                type: 'searchQuery',
                index: 'search_query',
                filters: {
                    lang: langFilter,
                    hasResults: {
                        field: 'hasResults',
                        termQuery: true,
                        defaultValue: true
                    }
                }
            }
        };

        const DefaultAutocomplete = {
            defaultType: '*',
            types: {
                searchQuery: {
                    // indexType: DefaultTypes.searchQuery,
                    queryFields: [
                        {
                            field: 'unicodeQuery',
                            vernacularOnly: true,
                            weight: 10
                        },
                        {
                            field: 'query',
                            weight: 9.5
                        }
                    ]
                }
            }
        };

        const DefaultSearch = {
            defaultType: '*'
        };

        const DefaultViews = {
            types: {
                searchQuery: {
                    // indexType: DefaultTypes.searchQuery,
                    sort: {count: true},
                    filters: {
                        hasResults: {
                            field: 'hasResults',
                            termQuery: true
                        }
                    }
                }
            }
        };

        const DefaultEventHandlers = {
            search: data => new SearchEventHandler(this.instanceName).handle(data)
        };

        const indices = config.searchConfig.indices || {};

        _.forEach(DefaultTypes, (type, key) => this.enhanceType(indices, key, type));
        _.forEach(config.searchConfig.types, (type, key) => this.enhanceType(indices, key, type));

        // TODO: compile config, so searcher logic has lesser checks
        // this.searchConfig = SearcherInternal.validateSearchConfig(config.searchConfig);
        this.searchConfig = _.defaultsDeep(config.searchConfig, {
            types: DefaultTypes,
            autocomplete: DefaultAutocomplete,
            search: DefaultSearch,
            views: DefaultViews
        });

        this.enhanceSearchTypes(_.get(this.searchConfig, ['autocomplete', 'types']), this.searchConfig);
        this.enhanceSearchTypes(_.get(this.searchConfig, ['search', 'types']), this.searchConfig);
        this.enhanceSearchTypes(_.get(this.searchConfig, ['views', 'types']), this.searchConfig);

        this.apiSchema = buildApiSchema(config.searchConfig);
        this.esClient = new ESClient(_.pick(config, ['logLevel', 'esConfig', 'redisConfig', 'redisSentinelConfig']));
        this.transliterator = config.transliterator;
        this.languageDetector = new LanguageDetector();

        this.eventEmitter = new EventEmitter();

        // this.registerEventHandlers(DefaultEventHandlers);
        this.registerEventHandlers(config.searchConfig.eventHandlers);
    }

    enhanceSearchTypes(types, searchConfig) {
        if (!types) {
            return;
        }

        _.forEach(types, (type, key) => {
            if (!type.indexType) {
                type.indexType = searchConfig.types[key];
            } else if (type.indexType && _.isString(type.indexType)) {
                type.indexType = searchConfig.types[type.indexType];
            } else if (_.isObject(type.indexType)) {
                type.indexType = _.defaultsDeep(type.indexType, searchConfig.types[key] || {});
            }
        });
    }

    enhanceType(indices, key, type) {
        if (!type.type) {
            type.type = key;
        }

        let index = indices[type.type];
        if (!index) {
            let indexStore = null;
            if (type.index) {
                indexStore = `${_.toLower(this.instanceName)}:${_.snakeCase(type.index)}_store`;
            } else {
                indexStore = `${_.toLower(this.instanceName)}_store`;
            }

            // we build index
            indices[type.type] = index = {
                store: indexStore
            };
        }

        type.index = index.store;

        if (!type.sort) {
            type.sort = [];
        }

        // add push by default
        type.sort.push('score');

        if (!type.filters) {
            type.filters = {};
        }

        if (!type.filters.lang) {
            type.filters.lang = langFilter;
        }
    }

    registerEventHandlers(eventHandlers) {
        if (!eventHandlers) {
            return;
        }

        _.forEach(eventHandlers, (handlerOrArray, eventName) => {
            if (_.isArray(handlerOrArray)) {
                _.forEach(handlerOrArray, handler => this.eventEmitter.addListener(eventName, handler));
            } else {
                this.eventEmitter.addListener(eventName, handlerOrArray);
            }
        });
    }

    // TODO: validate it through Joi
    // TODO: provide command line tool to validate config
    // validateSearchConfig(searchConfig) {
    //     return searchConfig;
    // }

    validateInput(input, schema) {
        if (!input) {
            throw new ValidationError('No input provided', {details: {code: 'NO_INPUT'}});
        }

        // validate it is valid type...
        const validationResult = Joi.validate(input, schema);
        if (validationResult.error) {
            let errorDetails = null;

            if (validationResult.error.details) {
                errorDetails = validationResult.error.details;
                if (_.isArray(errorDetails) && errorDetails.length === 1) {
                    errorDetails = errorDetails[0];
                }
            } else {
                errorDetails = validationResult.error;
            }

            throw new ValidationError('Non conforming format', {details: errorDetails});
        }

        return validationResult.value;
    }

    constantScoreQuery(fieldConfig, query) {
        if (fieldConfig.filter || query && (query.humane_query || query.multi_humane_query)) {
            return query;
        }

        const boost = (fieldConfig.weight || 1.0);

        if (boost === 1.0) {
            return query;
        }

        return {constant_score: {query, boost}};
    }

    wrapQuery(fieldConfig, query) {
        return this.constantScoreQuery(fieldConfig, fieldConfig.nestedPath ? {nested: {path: fieldConfig.nestedPath, query}} : query);
    }

    humaneQuery(fieldConfig, text, intentIndex, intentFields) {
        return {
            humane_query: {
                [fieldConfig.field]: {
                    intentIndex,
                    intentFields,
                    query: text,
                    boost: fieldConfig.weight,
                    vernacularOnly: fieldConfig.vernacularOnly,

                    //path: fieldConfig.nestedPath,
                    noFuzzy: fieldConfig.noFuzzy
                }
            }
        };
    }

    termQuery(fieldConfig, text) {
        const queryType = _.isArray(text) ? 'terms' : 'term';
        return {
            [queryType]: {
                [fieldConfig.field]: text
            }
        };
    }

    boolShouldQueries(queryArray) {
        if (queryArray.length === 0) {
            return null;
        }

        if (queryArray.length === 1) {
            return queryArray[0];
        }

        return {
            bool: {
                should: queryArray,
                minimum_should_match: 1
            }
        };
    }

    missingQuery(field) {
        return {
            bool: {
                must_not: {
                    exists: {
                        field
                    }
                }
            }
        };
    }

    buildFieldQuery(fieldConfig, englishTerm, queries, intentIndex, intentFields) {
        let query = null;

        if (fieldConfig.termQuery && fieldConfig.filter) {
            query = this.termQuery(fieldConfig, englishTerm);
        } else {
            query = this.humaneQuery(fieldConfig, englishTerm, intentIndex, intentFields);
        }

        query = this.wrapQuery(fieldConfig, query);

        if (fieldConfig.termQuery && fieldConfig.filter) {
            query = this.boolShouldQueries([query, this.wrapQuery(fieldConfig, this.missingQuery(fieldConfig.field))]);
        }

        if (queries) {
            if (_.isArray(query)) {
                _.forEach(query, (singleQuery) => queries.push(singleQuery));
            } else {
                queries.push(query);
            }
        }

        return query;
    }

    getIndexTypeConfigFromType(type) {
        const typeConfig = this.searchConfig.types[type];
        if (!typeConfig) {
            throw new ValidationError(`No index type config found for: ${type}`, {details: {code: 'INDEX_TYPE_NOT_FOUND', type}});
        }

        return typeConfig;
    }

    buildTypeQuery(searchTypeConfig, text, fuzzySearch, intentIndex, intentFields) {
        if (!text || _.isEmpty(text)) {
            return {};
        }

        // console.log('Fuzzy Search: ', fuzzySearch, !fuzzySearch || undefined);

        // // TODO: language detection is not needed immediately, but shall be moved to esplugin
        // const languages = this.languageDetector.detect(text);
        //
        // let englishTerm = text;
        // let vernacularTerm = null;
        // if (!(!languages || languages.length === 1 && languages[0].code === 'en') && this.transliterator) {
        //     // it's vernacular
        //     vernacularTerm = text;
        //     englishTerm = this.transliterator.transliterate(vernacularTerm);
        // }
        //
        // const indexTypeConfig = searchTypeConfig.indexType;
        //
        // const queries = [];
        // _.forEach(searchTypeConfig.queryFields || indexTypeConfig.queryFields, fieldConfig => this.buildFieldQuery(fieldConfig, englishTerm, vernacularTerm, queries));
        //
        // return {
        //     query: queries.length > 1 ? {dis_max: {queries}} : queries[0],
        //     language: languages && _.map(languages, lang => lang.code)
        // };

        const indexTypeConfig = searchTypeConfig.indexType;
        const queryFields = _(searchTypeConfig.queryFields || indexTypeConfig.queryFields).filter(queryField => !queryField.vernacularOnly).value();

        if (!queryFields) {
            throw new ValidationError('No query fields defined', {details: {code: 'NO_QUERY_FIELDS_DEFINED'}});
        } else if (queryFields.length === 1) {
            const queryField = queryFields[0];

            return {
                query: this.wrapQuery(queryField, {
                    humane_query: {
                        [queryField.field]: {
                            query: text,
                            boost: queryField.weight,
                            vernacularOnly: queryField.vernacularOnly,
                            noFuzzy: !fuzzySearch || queryField.noFuzzy,
                            intentIndex,
                            intentFields
                        }
                    }
                })
            };
        }
        return {
            query: {
                multi_humane_query: {
                    query: text,
                    intentIndex,
                    intentFields,
                    fields: _(queryFields)
                      .map(queryField => ({
                          field: queryField.field,
                          boost: queryField.weight,
                          vernacularOnly: queryField.vernacularOnly,
                          path: queryField.nestedPath,
                          noFuzzy: !fuzzySearch || queryField.noFuzzy
                      }))
                      .value()

                }
            }
        };
    }

    filterQueries(searchTypeConfig, input, termLanguages, intentIndex, intentFields) {
        const filterConfigs = searchTypeConfig.filters || searchTypeConfig.indexType.filters;

        if (!filterConfigs) {
            return undefined;
        }

        const filterQueries = [];

        _.forEach(filterConfigs, (filterConfig, key) => {
            if (filterConfig.type && filterConfig.type === 'post') {
                // skip post filters
                return true;
            }

            let filterValue = null;

            if (input.filter && input.filter[key]) {
                filterValue = input.filter[key];
            } else if (filterConfig.defaultValue) {
                filterValue = filterConfig.defaultValue;
            }

            if (filterValue && filterValue !== '__all__') {
                const filterType = filterValue.type;
                if (filterValue.values && filterType) {
                    filterValue = filterValue.values;
                }

                if (filterType && filterType === 'facet') {
                    return true;
                }

                if (filterConfig.value && _.isFunction(filterConfig.value)) {
                    filterValue = filterConfig.value(filterValue);
                }

                this.buildFieldQuery(_.extend({filter: true}, filterConfigs[key]), filterValue, filterQueries, intentIndex, intentFields);
            }

            return true;
        });

        if (input.lang && !_.isEmpty(input.lang)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), input.lang, filterQueries, intentIndex, intentFields);
        }

        if (termLanguages && !_.isEmpty(termLanguages)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), termLanguages, filterQueries, intentIndex, intentFields);
        }

        if (filterQueries.length === 0) {
            return undefined;
        }

        if (filterQueries.length === 1) {
            return filterQueries[0];
        }

        return {
            and: {
                filters: _.map(filterQueries, filter => ({query: filter}))
            }
        };
    }

    facetQueries(searchTypeConfig, input, intentIndex, intentFields) {
        const facetConfigs = searchTypeConfig.facets || searchTypeConfig.indexType.facets;

        if (!facetConfigs) {
            return undefined;
        }

        const facetQueries = [];

        _.forEach(facetConfigs, (facetConfig) => {
            const facetConfigKey = facetConfig.key;
            let filterValue = null;

            if (input.filter && input.filter[facetConfigKey]) {
                filterValue = input.filter[facetConfigKey];
            }

            if (filterValue && filterValue !== '__all__') {
                const filterType = filterValue.type;
                if (filterValue.values && filterType) {
                    filterValue = filterValue.values;
                }

                if (!filterType || filterType !== 'facet') {
                    return true;
                }

                if (facetConfig.type === 'field') {
                    // form field query here - termQuery, nestedPath, field
                    this.buildFieldQuery({filter: true, termQuery: true, field: facetConfig.field, nestedPath: facetConfig.nestedPath}, filterValue, facetQueries, intentIndex, intentFields);
                } else if (facetConfig.type === 'filters') {
                    // find matching filter and form appropriate query here
                    const matchingFilterQueries = [];
                    if (!_.isArray(filterValue)) {
                        filterValue = [filterValue];
                    }

                    _.forEach(filterValue, oneValue => {
                        _.forEach(facetConfig.filters, filterFacetConfig => {
                            if (oneValue === filterFacetConfig.key) {
                                matchingFilterQueries.push(filterFacetConfig.filter);
                            }
                        });
                    });

                    const query = this.boolShouldQueries(matchingFilterQueries);
                    if (query) {
                        facetQueries.push(query);
                    }
                } else if (facetConfig.type === 'ranges') {
                    // find matching range and form range query here
                    const matchingRangeQueries = [];
                    if (!_.isArray(filterValue)) {
                        filterValue = [filterValue];
                    }

                    _.forEach(filterValue, oneValue => {
                        _.forEach(facetConfig.ranges, filterRangeConfig => {
                            if (oneValue === filterRangeConfig.key) {
                                // TODO: support nested too
                                matchingRangeQueries.push({
                                    range: {
                                        [facetConfig.field]: {
                                            gte: filterRangeConfig.from,
                                            lt: filterRangeConfig.to
                                        }
                                    }
                                });
                            }
                        });
                    });

                    // push a missing data query here
                    matchingRangeQueries.push(this.missingQuery(facetConfig.field));

                    const query = this.boolShouldQueries(matchingRangeQueries);
                    if (query) {
                        facetQueries.push(query);
                    }
                }
            }

            return true;
        });

        if (facetQueries.length === 0) {
            return undefined;
        }

        if (facetQueries.length === 1) {
            return facetQueries[0];
        }

        return {
            and: {
                filters: _.map(facetQueries, filter => ({query: filter}))
            }
        };
    }

    // todo: see the usage of it...
    postFilters(searchTypeConfig, input) {
        const filterConfigs = searchTypeConfig.filters || searchTypeConfig.indexType.filters;

        if (!filterConfigs) {
            return undefined;
        }

        const postFilters = [];

        _.forEach(filterConfigs, (filterConfig, key) => {
            if (!filterConfig.type || filterConfig.type !== 'post') {
                // skip non filters
                return true;
            }

            let filterValue = null;

            if (input.filter && input.filter[key]) {
                filterValue = input.filter[key];
            } else if (filterConfig.defaultValue) {
                filterValue = filterConfig.defaultValue;
            }

            if (filterValue) {
                if (filterConfig.value && _.isFunction(filterConfig.value)) {
                    filterValue = filterConfig.value(filterValue);
                }

                postFilters.push(filterConfig.filter);
            }

            return true;
        });

        return postFilters;
    }

    defaultSortOrder() {
        return this.searchConfig.defaultSortOrder || Constants.DESC_SORT_ORDER;
    }

    sortOrder(order) {
        if (order) {
            return _.lowerCase(order);
        }

        return _.lowerCase(this.defaultSortOrder());
    }

    // todo: handle case of filtering only score based descending order, as it is default anyways
    buildSort(config, order) {
        // array of string
        if (_.isString(config)) {
            return {
                [config]: this.sortOrder(order)
            };
        } else if (_.isObject(config)) {
            if (config.sortFn && _.isFunction(config.sortFn)) {
                return config.sortFn(this.sortOrder(order));
            }
            return {
                [config.field]: this.sortOrder(order)
            };
        }

        return undefined;
    }

    buildDefaultSort(configOrArray) {
        // config is an array...
        return _(configOrArray)
          .filter(config => _.isObject(config) && config.default)
          .map(config => this.buildSort(config))
          .value();
    }

    sortPart(searchTypeConfig, input) {
        const sortConfigs = searchTypeConfig.sort || searchTypeConfig.indexType.sort;
        if (!sortConfigs || !_.isArray(sortConfigs)) {
            return undefined;
        }

        // build sort
        if (input.sort && input.sort.field) {
            const matchingConfig = _.find(sortConfigs, (config) => {
                if (_.isString(config) && config === input.sort.field
                  || _.isObject(config) && config.field === input.sort.field) {
                    return config;
                }

                return null;
            });

            if (matchingConfig) {
                return this.buildSort(matchingConfig, input.sort.order);
            }

            return undefined;
        }

        return this.buildDefaultSort(sortConfigs);
    }

    facetAggregation(aggregationConfig) {
        return {
            [aggregationConfig.type]: {field: aggregationConfig.field}
        };
    }

    facet(facetConfig, summariesConfig) {
        if (!facetConfig.key) {
            throw new ValidationError('No name defined for facet', {details: {code: 'NO_FACET_NAME_DEFINED'}});
        }

        if (!facetConfig.type) {
            throw new ValidationError('No facet type defined', {details: {code: 'NO_FACET_TYPE_DEFINED', facetName: facetConfig.key}});
        }

        if ((facetConfig.type === 'field' || facetConfig.type === 'ranges') && !facetConfig.field) {
            throw new ValidationError('No facet field defined', {details: {code: 'NO_FACET_FIELD_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
        }

        const facetKey = facetConfig.key;
        let facetValue = null;

        if (facetConfig.type === 'field') {
            facetValue = {
                terms: {
                    field: facetConfig.field,
                    size: 0
                }
            };
        } else if (facetConfig.type === 'ranges') {
            if (!facetConfig.ranges) {
                throw new ValidationError('No ranges defined for range type facet', {details: {code: 'NO_RANGES_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
            }

            facetValue = {
                range: {
                    field: facetConfig.field,
                    ranges: _.map(facetConfig.ranges, range => {
                        if (!range.key) {
                            throw new ValidationError('No range facet key defined', {details: {code: 'NO_RANGE_FACET_KEY_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
                        }

                        if (!range.from && !range.to) {
                            throw new ValidationError('None of range from & to defined', {details: {code: 'NO_RANGE_ENDS_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
                        }

                        return {
                            key: range.key,
                            from: range.from,
                            to: range.to
                        };
                    })
                }
            };
        } else if (facetConfig.type === 'filters') {
            if (!facetConfig.filters) {
                throw new ValidationError('No filters defined for filters type facet', {details: {code: 'NO_FILTERS_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
            }

            const filters = {};

            _.forEach(facetConfig.filters, filter => {
                filters[filter.key] = filter.filter;
            });

            facetValue = {
                filters: {filters}
            };
        } else {
            // throw error here
            throw new ValidationError('Unknown facet type', {details: {code: 'UNKNOWN_FACET_TYPE', facetName: facetConfig.key, facetType: facetConfig.type}});
        }

        if (summariesConfig) {
            const summaryAggregations = {};

            _.forEach(summariesConfig, (summaryConfig, summaryKey) => {
                summaryAggregations[summaryKey] = this.facetAggregation(summaryConfig);
            });

            facetValue.aggs = summaryAggregations;
        }

        if ((facetConfig.type === 'field' || facetConfig.type === 'ranges') && facetConfig.nestedPath) {
            facetValue = {
                nested: {
                    path: facetConfig.nestedPath
                },
                aggs: {
                    nested: facetValue
                }
            };
        }

        return {
            key: facetKey,
            value: facetValue
        };
    }

    // summaryFacet(facetKey, summaryConfig) {
    //     return {
    //         key: facetKey,
    //         value: this.facetAggregation(summaryConfig)
    //     };
    // }

    facetsPart(searchTypeConfig) {
        if (!searchTypeConfig.facets) {
            return null;
        }

        let facetConfigs = searchTypeConfig.facets;
        if (!_.isArray(facetConfigs)) {
            facetConfigs = [facetConfigs];
        }

        const facets = {};

        // if there is summaries
        // then build a special facet
        // also add the summary to each facet
        if (searchTypeConfig.summaries) {
            _.forEach(searchTypeConfig.summaries, (summaryConfig, summaryKey) => {
                const key = `__summary_${summaryKey}__`;
                facets[key] = this.facetAggregation(summaryConfig);
            });
        }

        _.forEach(facetConfigs, facetConfig => {
            const facet = this.facet(facetConfig, searchTypeConfig.summaries);
            facets[facet.key] = facet.value;
        });

        return facets;
    }

    searchQuery(searchTypeConfig, input, intentIndex, intentFields) {
        let text = input.text;

        if ((this.instanceName === '1mg' || this.instanceName === 'netmeds') && text) {
            // fix text
            text = _(text)
              .replace(/(^|[\s]|[^0-9]|[^a-z])([0-9]+)[\s]+(mg|mcg|ml|%)/gi, '$1$2$3')
              .replace(/(^|[\s]|[^0-9]|[^a-z])\.([0-9]+)[\s]*(mg|mcg|ml|%)/gi, '$10.$2$3')
              .replace(/([0-9]+)'S$/gi, '$1S')
              .trim();
        }

        return Promise.resolve(this.buildTypeQuery(searchTypeConfig, text, input.fuzzySearch, intentIndex, intentFields))
          .then(({query, queryLanguages}) => {
              const indexTypeConfig = searchTypeConfig.indexType;

              let sort = this.sortPart(searchTypeConfig, input) || undefined;
              if (sort && _.isEmpty(sort)) {
                  sort = undefined;
              }

              let facets = this.facetsPart(searchTypeConfig) || undefined;
              if (facets && _.isEmpty(facets)) {
                  facets = undefined;
              }

              return {
                  index: indexTypeConfig.index,
                  type: indexTypeConfig.type,
                  search: {
                      from: (input.page || 0) * (input.count || 0),
                      size: input.count || undefined,
                      sort,
                      query: {
                          function_score: {
                              query: {
                                  bool: {
                                      must: query || {
                                          match_all: {}
                                      },
                                      filter: this.filterQueries(searchTypeConfig, input, _.keys(queryLanguages), intentIndex, intentFields)
                                  }
                              },
                              field_value_factor: {
                                  field: '_weight',
                                  factor: 2.0,
                                  missing: 1
                              }
                          }
                      },
                      post_filter: this.facetQueries(searchTypeConfig, input, intentIndex, intentFields),
                      aggs: facets
                  },
                  queryLanguages
              };
          });
    }

    _deepOmit(source) {
        if (_.isArray(source)) {
            return _.map(source, value => this._deepOmit(value));
        } else if (_.isObject(source) && !_.isDate(source) && !_.isRegExp(source) && !_.isFunction(source)) {
            return _(source)
              .omitBy((value, key) => {
                  if (_.startsWith(key, '__') && _.endsWith(key, '__')) {
                      return true;
                  }

                  if (this.instanceName === 'carDekho' && (key === 'features' || key === 'colors' || key === 'content')) {
                      return true;
                  }

                  return false;
              })
              .mapValues((value) => this._deepOmit(value))
              .value();
        }

        return source;
    }

    _processSource(hit, name) {
        if (!hit._source) {
            return null;
        }

        let source = this._deepOmit(hit._source);

        source = _(source)
          .mapValues((value, key) => {
              if (key === '_hourlyStats' || key === '_dailyStats' || key === '_weeklyStats' || key === '_monthlyStats') {
                  return _.mapValues(value, childValue => _.omit(childValue, 'lastNStats'));
              }

              return value;
          })
          .value();

        return _.defaults(_.pick(hit, ['_id', '_score', '_type', '_weight']), {_name: name}, source);
    }

    _processResponse(response, searchTypesConfig, type) {
        // let type = null;
        let name = null;

        // console.log('========> Process Response Type: ', type, searchTypesConfig);

        const results = [];

        if (response.hits && response.hits.hits) {
            let first = true;
            _.forEach(response.hits.hits, hit => {
                if (first || !type) {
                    type = hit._type;
                    const typeConfig = this.searchConfig.types[type];
                    name = (typeConfig && (typeConfig.name || typeConfig.type)) || type;

                    first = false;
                }

                results.push(this._processSource(hit, name));
            });
        }

        const searchTypeConfig = type && searchTypesConfig[type];

        let summaryKeys = null;
        if (searchTypeConfig && searchTypeConfig.summaries) {
            summaryKeys = _.chain(searchTypeConfig.summaries).keys().value();
        }

        let summaries;
        if (summaryKeys && response.aggregations) {
            summaries = {};
            _.forEach(summaryKeys, summaryKey => {
                const summary = _.get(response.aggregations, [`__summary_${summaryKey}__`, 'value']);
                if (!_.isUndefined(summary)) {
                    summaries[summaryKey] = summary;
                }
            });
        }

        let facets;
        if (searchTypeConfig && searchTypeConfig.facets && response.aggregations) {
            facets = {};
            let facetConfigs = searchTypeConfig.facets;
            if (!_.isArray(facetConfigs)) {
                facetConfigs = [facetConfigs];
            }

            _.forEach(facetConfigs, facetConfig => {
                let facet = response.aggregations[facetConfig.key];

                if (!facet) {
                    return true;
                }

                if ((facetConfig.type === 'field' || facetConfig.type === 'ranges') && facetConfig.nestedPath) {
                    facet = facet.nested;
                }

                const buckets = facet.buckets;

                if (facetConfig.type === 'filter' || facetConfig.type === 'filters') {
                    // bucket is an object with key as object key and doc_count as value
                    const output = facets[facetConfig.key] = [];

                    _.forEach(buckets, (bucket, key) => {
                        const facetResult = {
                            key,
                            count: bucket.doc_count,
                            from: facet.from,
                            from_as_string: facet.from_as_string,
                            to: facet.to,
                            to_as_string: facet.to_as_string
                        };

                        if (summaryKeys) {
                            _.forEach(summaryKeys, summaryKey => {
                                facetResult[summaryKey] = _.get(bucket, [summaryKey, 'value']);
                            });
                        }

                        output.push(facetResult);
                    });
                } else {
                    // bucket is an array of objects with key and doc_count
                    facets[facetConfig.key] = _.map(buckets, bucket => {
                        const facetResult = {
                            key: bucket.key,
                            count: bucket.doc_count,
                            from: facet.from,
                            from_as_string: facet.from_as_string,
                            to: facet.to,
                            to_as_string: facet.to_as_string
                        };

                        if (summaryKeys) {
                            _.forEach(summaryKeys, summaryKey => {
                                facetResult[summaryKey] = _.get(bucket, [summaryKey, 'value']);
                            });
                        }

                        return facetResult;
                    });
                }

                return true;
            });
        }

        return {type, name, resultType: type, results, summaries, facets, queryTimeTaken: response.took, totalResults: _.get(response, 'hits.total', 0)};
    }

    processMultipleSearchResponse(responses, searchTypesConfig, types, input) {
        if (!responses) {
            return null;
        }

        const mergedResult = {
            multi: true,
            totalResults: 0,
            results: {},
            searchText: input.text,
            filter: input.filter,
            sort: input.sort,
            page: input.page,
            count: 0
        };

        _.forEach(responses.responses, (response, index) => {
            const type = types && _.isArray(types) && types.length > index && types[index];
            const result = this._processResponse(response,
              searchTypesConfig,
              type);

            if (!result || !result.type || !result.name || !result.results || result.results.length === 0) {
                return;
            }

            mergedResult.queryTimeTaken = Math.max(mergedResult.queryTimeTaken || 0, result.queryTimeTaken);
            mergedResult.results[result.name] = result;
            mergedResult.totalResults += result.totalResults;
            mergedResult.count += result && result.length;
        });

        return mergedResult;
    }

    processSingleSearchResponse(response, searchTypesConfig, type, input) {
        if (!response) {
            return null;
        }

        const finalResponse = this._processResponse(response, searchTypesConfig, type);

        return _.extend(finalResponse, {
            searchText: input.text,
            filter: input.filter,
            sort: input.sort,
            page: input.page,
            count: finalResponse && finalResponse.results && finalResponse.results.length
        });
    }

    _searchInternal(headers, input, searchApiConfig, eventName) {
        let queryLanguages = null;

        let multiSearch = false;

        let promise = null;

        const searchTypeConfigs = searchApiConfig.types;

        let responsePostProcessor = null;

        let typeOrTypesArray = null;

        const intentIndex = `${_.toLower(this.instanceName)}:intent_store`;
        let intentFields = [];
        if (!input.type || input.type === '*') {
            responsePostProcessor = searchApiConfig.multiResponsePostProcessor;

            typeOrTypesArray = [];
            _(searchTypeConfigs)
              .values()
              .forEach(searchTypeConfig => {
                  typeOrTypesArray.push(_.get(searchTypeConfig, 'indexType.type'));
                  intentFields = _.concat(intentFields, _.get(searchTypeConfig, 'intentEntities', []));
              });

            intentFields = _.uniq(intentFields);

            const searchQueries = _(searchTypeConfigs)
              .values()
              .map(typeConfig => this.searchQuery(typeConfig, input, intentIndex, intentFields))
              .value();

            multiSearch = _.isArray(searchQueries) || false;

            promise = Promise.all(searchQueries);
        } else {
            const searchTypeConfig = searchTypeConfigs[input.type];

            if (!searchTypeConfig) {
                throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'SEARCH_CONFIG_NOT_FOUND', type: input.type}});
            }

            typeOrTypesArray = searchTypeConfig.indexType && searchTypeConfig.indexType.type;
            intentFields = _.concat(intentFields, _.get(searchTypeConfig, 'intentEntities', []));

            responsePostProcessor = searchTypeConfig.responsePostProcessor;

            intentFields = _.uniq(intentFields);

            promise = this.searchQuery(searchTypeConfig, input, intentIndex, intentFields);
        }

        return Promise.resolve(promise)
          .then(queryOrArray => {
              if (multiSearch) {
                  queryLanguages = _.head(queryOrArray).queryLanguages;
              } else {
                  queryLanguages = queryOrArray.queryLanguages;
              }

              return queryOrArray;
          })
          .then(queryOrArray => {
              if (multiSearch) {
                  return this.esClient.multiSearch(queryOrArray);
              }

              return this.esClient.search(queryOrArray);
          })
          .then((response) => {
              if (multiSearch) {
                  return this.processMultipleSearchResponse(response, searchTypeConfigs, typeOrTypesArray, input);
              }

              return this.processSingleSearchResponse(response, searchTypeConfigs, typeOrTypesArray, input);
          })
          .then(response => {
              this.eventEmitter.emit(eventName, {headers, queryData: input, queryLanguages, queryResult: response});

              if (responsePostProcessor && input.format === 'custom') {
                  return responsePostProcessor(response);
              }

              return response;
          });
    }

    // build
    //      intent index
    //      lookUpEntities
    //      regexEntities - todo later
    //      query
    _intentInternal(headers, input, searchApiConfig) {
        const intentIndex = `${_.toLower(this.instanceName)}:intent_store`;
        const query = input.text;

        const searchTypeConfigs = searchApiConfig.types;

        // TODO: find lookup entities
        // TODO: filter and map these with
        let intentFields = [];
        if (!input.type || input.type === '*') {
            _(searchTypeConfigs)
              .values()
              .forEach(searchTypeConfig => {
                  intentFields = _.concat(intentFields, _.get(searchTypeConfig, 'intentEntities', []));
              });
        } else {
            const searchTypeConfig = searchTypeConfigs[input.type];

            if (!searchTypeConfig) {
                throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'SEARCH_CONFIG_NOT_FOUND', type: input.type}});
            }

            intentFields = _.concat(intentFields, _.get(searchTypeConfig, 'intentEntities', []));
        }

        intentFields = _.uniq(intentFields);

        const lookupEntities = _(intentFields)
          .uniq()
          .map(intentField => ({name: intentField, value: _.get(this.searchConfig, ['lookupIntentEntities', intentField])}))
          .filter(value => !!value.value)
          .map(value => (_.defaults({name: value.name}, value.value)))
          .value();

        const intentQuery = {query, lookupEntities};

        console.log('Intent Query: ', JSON.stringify(intentQuery, null, 2));

        // form the request and get a response
        return Promise.resolve(this.esClient.intent(intentIndex, intentQuery));
    }

    intent(headers, input) {
        // TODO: do validation later
        return this._intentInternal(headers, input);
    }

    autocomplete(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.autocomplete);

        return this._searchInternal(headers, validatedInput, this.searchConfig.autocomplete, Constants.AUTOCOMPLETE_EVENT);
    }

    formSearch(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.formSearch);

        return this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.FORM_SEARCH_EVENT);
    }

    browseAll(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.browseAll);

        return this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.BROWSE_ALL_EVENT);
    }

    buildSearchSections(text, sectionQueries) {
        return Promise.props(sectionQueries)
          .then(response => {
              // combine all results into one and return sections
              const finalResult = {
                  searchText: text,
                  results: [],
                  totalResults: 0
              };

              _.forEach(response, (value, key) => {
                  const section = _.defaults({type: 'section', name: key}, value);

                  if (value.count === 1) {
                      section.mode = 'single';
                  }

                  finalResult.totalResults += _.get(section, 'totalResults', 0);
                  finalResult.results.push(section);
              });

              return finalResult;
          });
    }

    buildIntentTokenQuery(intentToken, weight, field) {
        let fieldSuffix = 'humane';
        if (intentToken.match_token_type === 'Bi') {
            // form a shingle field query
            fieldSuffix = 'shingle';
        }

        return {
            bool: {
                should: [
                    {
                        term: {
                            [`${field}.${fieldSuffix}`]: {
                                value: intentToken.token,
                                boost: 10.0 * weight * intentToken.score
                            }
                        }
                    },
                    {
                        term: {
                            [`${field}.${fieldSuffix}`]: {
                                value: `e#${intentToken.token}`,
                                boost: 2.0 * weight * intentToken.score
                            }
                        }
                    }
                ],
                minimum_should_match: 1
            }
        };
    }

    buildIntentTokenListQuery(intentTokenList, weight, field) {
        if (intentTokenList.length > 1) {
            return {
                bool: {
                    should: _.map(intentTokenList, intentToken => this.buildIntentTokenQuery(intentToken, weight, field)),
                    minimum_should_match: intentTokenList.length
                }
            };
        }

        return this.buildIntentTokenQuery(intentTokenList[0], weight, field);
    }

    buildIntentSuggestionQuery(intentSuggestion, field) {
        // make a boolean or query
        // find min should match
        return this.buildIntentTokenListQuery(intentSuggestion.intent_tokens, intentSuggestion.score, field);
    }

    buildIntentSuggestionListQuery(intentSuggestionList, field) {
        // make a dis-max of intent suggestion query
        if (intentSuggestionList.length > 1) {
            return {
                dis_max: {
                    tie_breaker: 0.7,
                    boost: 1.2,
                    queries: _.map(intentSuggestionList, intentSuggestion =>
                      this.buildIntentSuggestionQuery(intentSuggestion, field))
                }
            };
        }

        return this.buildIntentSuggestionQuery(intentSuggestionList[0], field);
    }

    executeIntentSuggestionListQuery(intentSuggestionList, type, field) {
        return Promise.resolve(this.esClient.search({
            index: `${_.toLower(this.instanceName)}_store`,
            type,
            search: {
                query: this.buildIntentSuggestionListQuery(intentSuggestionList, field)
            }
        }))
          .then(response => this._processResponse(response, this.searchConfig.search.types, type));
    }

    searchCarDekhoBrand(intentSuggestions) {
        return this.executeIntentSuggestionListQuery(intentSuggestions, 'new_car_brand', 'brand');
    }

    searchCarDekhoModel(intentSuggestions) {
        return this.executeIntentSuggestionListQuery(intentSuggestions, 'new_car_model', 'model');
    }

    searchCarDekhoVariant(intentSuggestions) {
        return this.executeIntentSuggestionListQuery(intentSuggestions, 'new_car_variant', 'variant');
    }

    searchUsedCarsByIntentSuggestions(intentSuggestions, field) {
        return this.executeIntentSuggestionListQuery(intentSuggestions, 'used_car', field);
    }

    searchNewsByIntentSuggestions(intentSuggestions, field) {
        return this.executeIntentSuggestionListQuery(intentSuggestions, 'car_news', field);
    }

    searchNewCarDealersByIntentSuggestions(intentSuggestions, field) {
        return this.executeIntentSuggestionListQuery(intentSuggestions, 'new_car_dealer', field);
    }

    searchUsedCarsByMatchingBrands() {

    }

    searchUsedCarsByMatchingModelsOrBrands() {

    }

    searchNewsByMatchingBrands() {

    }

    searchNewsByMatchingModels() {

    }

    searchNewCarDealersByMatchingBrands() {

    }

    buildSection(sectionResponseOrPromise, sectionName, sectionType, sectionTitle) {
        return Promise.resolve(sectionResponseOrPromise)
          .then(response => (_.defaults({type: 'section', name: sectionName, title: sectionTitle, resultType: sectionType}, response)));
    }

    composeSections(...sections) {
        return Promise.all(sections)
          .then(responses => {
              let totalResults = 0;
              const results = [];
              _.forEach(responses, response => {
                  if (response) {
                      results.add(response);
                      totalResults += _.get(response, 'totalResults', 0);
                  }
              });

              return {
                  results,
                  totalResults
              };
          });
    }

    formatMultiResponseIntoSections(multiResponseOrPromise) {
        return Promise.resolve(multiResponseOrPromise)
          .then(multiResponse => this.composeSections(
            this.buildSection(_.get(multiResponse, 'results.new_car_model'), 'models', 'new_car_model', 'New Car Models'),
            this.buildSection(_.get(multiResponse, 'results.new_car_variant'), 'variants', 'new_car_variant', 'New Car Variants'),
            this.buildSection(_.get(multiResponse, 'results.used_car'), 'used-cars', 'used_car', 'Used Cars'),
            this.buildSection(_.get(multiResponse, 'results.car_news'), 'news', 'car_news', 'News'),
            this.buildSection(_.get(multiResponse, 'results.new_car_dealer'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
          ));
    }

    carDekhoIntentBasedSearch(intentSuggestions, headers, input, searchApiConfig) {
        return Promise.props({
            brand: this.searchCarDekhoBrand(intentSuggestions),
            model: this.searchCarDekhoModel(intentSuggestions),
            variant: this.searchCarDekhoVariant(intentSuggestions)
        })
          .then(response => {
              // check if brand result and how many
              // else check if model result and how many
              // else check if variant results and how many
              const brandHits = _.get(response, 'brand.totalResults', 0);
              const modelHits = _.get(response, 'model.totalResults', 0);
              const variantHits = _.get(response, 'variant.totalResults', 0);

              let searchType = null;
              if (brandHits) {
                  if (brandHits === 1) {
                      searchType = 'brand-single';
                  } else {
                      searchType = 'brand-multi';
                  }
              } else if (modelHits) {
                  if (modelHits === 1) {
                      searchType = 'model-single';
                  } else {
                      searchType = 'model-multi';
                  }
              } else if (variantHits) {
                  if (variantHits === 1) {
                      searchType = 'variant-single';
                  } else {
                      searchType = 'variant-multi';
                  }
              }

              if (!searchType) {
                  return this.formatMultiResponseIntoSections(this._searchInternal(headers, input, searchApiConfig, Constants.SEARCH_EVENT));
              }

              if (searchType === 'brand-single') {
                  // model search for brand
                  // used car search for brand
                  // news search for brand
                  // new car dealers for brand

                  // return this.buildSearchSections(validatedInput.text, {
                  //     models: this.browseAll(headers, {type: 'new_car_model'}), // use existing results for models
                  //     'used-cars': this.browseAll(headers, {type: 'used_car'}), // make simple query with intentSuggestions, field = brand
                  //     news: this.browseAll(headers, {type: 'car_news'}), // make simple query with intentSuggestions, field = brand
                  //     'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'}) // make simple query with intentSuggestions, field = brand
                  // });

                  return this.composeSections(
                    this.buildSection(response.model, 'models', 'new_car_model', 'New Cars'),
                    this.buildSection(this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'brand'), 'used-cars', 'used_car', 'Used Cars'),
                    this.buildSection(this.searchNewsByIntentSuggestions(intentSuggestions, 'brand'), 'news', 'car_news', 'News'),
                    this.buildSection(this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                  );
              } else if (searchType === 'brand-multi') {
                  // matching brands
                  // used car search for matching brands
                  // news search for matching brands
                  // new car dealers for matching brands

                  // return this.buildSearchSections(validatedInput.text, {
                  //     'matching-brands': this.browseAll(headers, {type: 'new_car_brand'}), // use existing results for brands
                  //     'used-cars': this.browseAll(headers, {type: 'used_car'}), // make simple query with intentSuggestions, field = brand
                  //     news: this.browseAll(headers, {type: 'car_news'}), // make simple query with intent suggestions, field = brand
                  //     'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'}) // make simple query with intent suggestions, field = brand
                  // });

                  return this.composeSections(
                    this.buildSection(response.brand, 'matching-brands', 'new_car_brand', 'Matching Brands'),
                    this.buildSection(this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'brand'), 'used-cars', 'used_car', 'Used Cars'),
                    this.buildSection(this.searchNewsByIntentSuggestions(intentSuggestions, 'brand'), 'news', 'car_news', 'News'),
                    this.buildSection(this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                  );
              } else if (searchType === 'model-single') {
                  // matching single model
                  // variants for matching model
                  // used cars for matching model or brand of the matching model
                  // news for matching model or brand of the matching model
                  // new car dealers for brand of the matching model

                  // return this.buildSearchSections(validatedInput.text, {
                  //     model: this.browseAll(headers, {type: 'new_car_model', count: 1}), // use existing results for models
                  //     variants: this.browseAll(headers, {type: 'new_car_variant'}), // use existing results for variants
                  //     'used-cars': this.browseAll(headers, {type: 'used_car'}), // make search for intent suggestions (field = model) or brand output
                  //     news: this.browseAll(headers, {type: 'car_news'}), // make search for intent suggestions (field = model) or brand output (field = brand)
                  //     'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'}) // make search for brand output (field = brand)
                  // });

                  return this.composeSections(
                    this.buildSection(response.model, 'model', 'new_car_model', 'Matching Model'),
                    this.buildSection(response.variant, 'variants', 'new_car_variant', 'New Cars'),
                    this.buildSection(this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'), 'used-cars', 'used_car', 'Used Cars'),
                    this.buildSection(this.searchNewsByIntentSuggestions(intentSuggestions, 'model'), 'news', 'car_news', 'News'),
                    this.buildSection(this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                  );
              } else if (searchType === 'model-multi') {
                  // matching models
                  // used cars for matching models or brand of the matching models
                  // news for matching models or brand of the matching models
                  // new car dealers for brand of the matching models

                  // return this.buildSearchSections(validatedInput.text, {
                  //     'matching-models': this.browseAll(headers, {type: 'new_car_model'}), // use existing results for models
                  //     'used-cars': this.browseAll(headers, {type: 'used_car'}), // make search for intent suggestions (field = model) or brand output
                  //     news: this.browseAll(headers, {type: 'car_news'}), // make search for intent suggestions (field = model) or brand output (field = brand)
                  //     'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'}) // make search for brand output (field = brand)
                  // });

                  return this.composeSections(
                    this.buildSection(response.model, 'matching-models', 'new_car_model', 'Matching Models'),
                    this.buildSection(this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'), 'used-cars', 'used_car', 'Used Cars'),
                    this.buildSection(this.searchNewsByIntentSuggestions(intentSuggestions, 'model'), 'news', 'car_news', 'News'),
                    this.buildSection(this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                  );
              } else if (searchType === 'variant-single') {
                  // matching single variant
                  // similar variants
                  // used cars for models of the matching variant
                  // news for models of the matching variant
                  // new car dealers for brand of the matching variant

                  // return this.buildSearchSections(validatedInput.text, {
                  //     variant: this.browseAll(headers, {type: 'new_car_model', count: 1}), // use existing results for variants
                  //     'similar-variants': this.browseAll(headers, {type: 'new_car_variant'}), // do not do it now
                  //     'used-cars': this.browseAll(headers, {type: 'used_car'}), // make search for model output (field = model)
                  //     news: this.browseAll(headers, {type: 'car_news'}), // make search for model output (field = model)
                  //     'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'}) // make search for brand output (field = brand)
                  // });

                  return this.composeSections(
                    this.buildSection(response.variant, 'variant', 'new_car_variant', 'Matching Variant'),
                    this.buildSection(this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'), 'used-cars', 'used_car', 'Used Cars'),
                    this.buildSection(this.searchNewsByIntentSuggestions(intentSuggestions, 'model'), 'news', 'car_news', 'News'),
                    this.buildSection(this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                  );
              } //else /*if (searchType === 'variant-multi')*/ {
              // matching variants
              // used cars for models of the matching variants
              // news for models of the matching variants
              // new car dealers for brand of the matching variants

              // return this.buildSearchSections(validatedInput.text, {
              //     'matching-variants': this.browseAll(headers, {type: 'new_car_variant'}), // use existing results for variants
              //     'used-cars': this.browseAll(headers, {type: 'used_car'}), // make search for model output (field = model)
              //     news: this.browseAll(headers, {type: 'car_news'}), // make search for model output (field = model)
              //     'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'}) // make search for brand output (field = brand)
              // });

              return this.composeSections(
                this.buildSection(response.variant, 'matching-variants', 'new_car_variant', 'Matching Variants'),
                this.buildSection(this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'), 'used-cars', 'used_car', 'Used Cars'),
                this.buildSection(this.searchNewsByIntentSuggestions(intentSuggestions, 'model'), 'news', 'car_news', 'News'),
                this.buildSection(this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
              );
              //}
          });
    }

    search(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.search);

        if (this.instanceName === 'carDekho' && (!validatedInput.type || validatedInput.type === '*')) {
            // if (validatedInput.text === 'brand-single') {
            //     // model search for brand
            //     // used car search for brand
            //     // news search for brand
            //     // new car dealers for brand
            //     // used car dealers for brand
            //     // service centers for brand
            //     return this.buildSearchSections(validatedInput.text, {
            //         models: this.browseAll(headers, {type: 'new_car_model'}),
            //         'used-cars': this.browseAll(headers, {type: 'used_car'}),
            //         news: this.browseAll(headers, {type: 'car_news'}),
            //         'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'})
            //         // 'used-car-dealers': this.browseAll(headers, {type: 'used_car_dealer'})
            //         // 'service-centers': this.browseAll(headers, {type: 'service_center'})
            //     });
            // } else if (validatedInput.text === 'brand-multi') {
            //     // matching brands
            //     // used car search for matching brands
            //     // news search for matching brands
            //     // new car dealers for matching brands
            //     // used car dealers for matching brands
            //     // service centers for matching brands
            //     return this.buildSearchSections(validatedInput.text, {
            //         'matching-brands': this.browseAll(headers, {type: 'new_car_brand'}),
            //         'used-cars': this.browseAll(headers, {type: 'used_car'}),
            //         news: this.browseAll(headers, {type: 'car_news'}),
            //         'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'})
            //         // 'used-car-dealers': this.browseAll(headers, {type: 'used_car_dealer'})
            //         // 'service-centers': this.browseAll(headers, {type: 'service_center'})
            //     });
            // } else if (validatedInput.text === 'model-single') {
            //     // matching single model
            //     // variants for matching model
            //     // used cars for matching model or brand of the matching model
            //     // news for matching model or brand of the matching model
            //     // new car dealers for brand of the matching model
            //     // used car dealers for brand of the matching model
            //     // service centers for brand of brand of the matching model
            //     return this.buildSearchSections(validatedInput.text, {
            //         model: this.browseAll(headers, {type: 'new_car_model', count: 1}),
            //         variants: this.browseAll(headers, {type: 'new_car_variant'}),
            //         'used-cars': this.browseAll(headers, {type: 'used_car'}),
            //         news: this.browseAll(headers, {type: 'car_news'}),
            //         'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'})
            //         // 'used-car-dealers': this.browseAll(headers, {type: 'used_car_dealer'})
            //         // 'service-centers': this.browseAll(headers, {type: 'service_center'})
            //     });
            // } else if (validatedInput.text === 'model-multi') {
            //     // matching models
            //     // used cars for matching models or brand of the matching models
            //     // news for matching models or brand of the matching models
            //     // new car dealers for brand of the matching models
            //     // used car dealers for brand of the matching models
            //     // service centers for brand of brand of the matching models
            //     return this.buildSearchSections(validatedInput.text, {
            //         'matching-models': this.browseAll(headers, {type: 'new_car_model'}),
            //         'used-cars': this.browseAll(headers, {type: 'used_car'}),
            //         news: this.browseAll(headers, {type: 'car_news'}),
            //         'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'})
            //         // 'used-car-dealers': this.browseAll(headers, {type: 'used_car_dealer'})
            //         // 'service-centers': this.browseAll(headers, {type: 'service_center'})
            //     });
            // } else if (validatedInput.text === 'variant-single') {
            //     // matching single variant
            //     // similar variants
            //     // used cars for models of the matching variant
            //     // news for models of the matching variant
            //     // new car dealers for brand of the matching variant
            //     // used car dealers for brand of the matching variant
            //     // service centers for brand of brand of the matching variant
            //     return this.buildSearchSections(validatedInput.text, {
            //         variant: this.browseAll(headers, {type: 'new_car_model', count: 1}),
            //         'similar-variants': this.browseAll(headers, {type: 'new_car_variant'}),
            //         'used-cars': this.browseAll(headers, {type: 'used_car'}),
            //         news: this.browseAll(headers, {type: 'car_news'}),
            //         'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'})
            //         // 'used-car-dealers': this.browseAll(headers, {type: 'used_car_dealer'})
            //         // 'service-centers': this.browseAll(headers, {type: 'service_center'})
            //     });
            // } else if (validatedInput.text === 'variant-multi') {
            //     // matching variants
            //     // used cars for models of the matching variants
            //     // news for models of the matching variants
            //     // new car dealers for brand of the matching variants
            //     // used car dealers for brand of the matching variants
            //     // service centers for brand of brand of the matching variants
            //     return this.buildSearchSections(validatedInput.text, {
            //         'matching-variants': this.browseAll(headers, {type: 'new_car_variant'}),
            //         'used-cars': this.browseAll(headers, {type: 'used_car'}),
            //         news: this.browseAll(headers, {type: 'car_news'}),
            //         'new-car-dealers': this.browseAll(headers, {type: 'new_car_dealer'})
            //         // 'used-car-dealers': this.browseAll(headers, {type: 'used_car_dealer'})
            //         // 'service-centers': this.browseAll(headers, {type: 'service_center'})
            //     });
            // } else if (validatedInput.text === 'model-link-single') {
            //     // single matching link
            //     // related links
            //     return this.buildSearchSections(validatedInput.text, {
            //         link: this.browseAll(headers, {type: 'new_car_model_page', count: 1}),
            //         'related-links': this.browseAll(headers, {type: 'new_car_model_page'})
            //     });
            // } else if (validatedInput.text === 'model-link-multi') {
            //     // matching links
            //     return this.buildSearchSections(validatedInput.text, {
            //         'matching-links': this.browseAll(headers, {type: 'new_car_model_page'})
            //     });
            // }

            return Promise.resolve(this._intentInternal(headers, input, this.searchConfig.search))
              .then(response => {
                  // console.log('Intent Response: ', JSON.stringify(response, null, 2));
                  if (_.isEmpty(response.results)) {
                      return this.formatMultiResponseIntoSections(this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.SEARCH_EVENT));
                  }

                  // get the first result for now
                  const intentResult = _.first(response.results);
                  const intentSuggestions = _.get(intentResult, ['intent_classes', 'car_name']);
                  if (!intentSuggestions || _.isEmpty(intentSuggestions)) {
                      return this.formatMultiResponseIntoSections(this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.SEARCH_EVENT));
                  }

                  // make a query for brand, model, or variant
                  return this.carDekhoIntentBasedSearch(intentSuggestions, headers, validatedInput, this.searchConfig.search);
              });
        }

        return this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.SEARCH_EVENT);
    }

    didYouMean(headers, input) {
        // const validatedInput = this.validateInput(input, this.apiSchema.didYouMean);
        //
        // const types = this.searchConfig.types;
        //
        // let index = null;
        // if (!input.type || input.type === '*') {
        //     index = _(types)
        //       .map(typeConfig => typeConfig.index)
        //       .filter(indexName => !indexName.match(/search_query_store/))
        //       .join(',');
        // } else {
        //     const typeConfig = types[input.type];
        //     if (!typeConfig) {
        //         throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'TYPE_CONFIG_NOT_FOUND', type: input.type}});
        //     }
        //
        //     index = typeConfig.index;
        // }
        //
        // let text = validatedInput.text;
        // if ((this.instanceName === '1mg' || this.instanceName === 'netmeds') && text) {
        //     // fix text
        //     text = _(text)
        //       .replace(/(^|[\s]|[^0-9]|[^a-z])([0-9]+)[\s]+(mg|mcg|ml|%)/gi, '$1$2$3')
        //       .replace(/(^|[\s]|[^0-9]|[^a-z])\.([0-9]+)[\s]*(mg|mcg|ml|%)/gi, '$10.$2$3')
        //       .replace(/([0-9]+)'S$/gi, '$1S')
        //       .trim();
        // }
        //
        // return Promise.resolve(this.esClient.didYouMean(index, text));
        return null;
    }

    suggestedQueries(headers, input) {
        // same type as autocomplete
        const validatedInput = this.validateInput(input, this.apiSchema.autocomplete);

        return this._searchInternal(headers, validatedInput, this.searchConfig.autocomplete, Constants.SUGGESTED_QUERIES_EVENT)
          .then(response => {
              // merge
              if (response.multi) {
                  // calculate scores
                  const relevancyScores = [];
                  _.forEach(response.results, resultGroup => {
                      _.forEach(resultGroup.results, result => {
                          result._relevancyScore = result._score / (result._weight || 1.0);
                          relevancyScores.push(result._relevancyScore);
                      });
                  });

                  // order scores in descending order
                  relevancyScores.sort((scoreA, scoreB) => scoreB - scoreA);

                  // find deflection point
                  let previousScore = 0;
                  let deflectionScore = 0;
                  _.forEach(relevancyScores, score => {
                      if (previousScore && score < 0.5 * previousScore) {
                          deflectionScore = previousScore;
                          return false;
                      }

                      previousScore = score;

                      return true;
                  });

                  // consider items till the deflection point
                  const results = [];
                  _.forEach(response.results, resultGroup => {
                      _.forEach(resultGroup.results, result => {
                          if (result._relevancyScore >= deflectionScore) {
                              results.push(result);
                          }
                      });
                  });

                  results.sort((resultA, resultB) => resultB._score - resultA._score);

                  response.results = results;
              }

              return response;
          });
    }

    _explain(api, input) {
        let apiConfig = null;
        if (api === Constants.AUTOCOMPLETE_API) {
            apiConfig = this.searchConfig.autocomplete;
        } else if (api === Constants.SEARCH_API) {
            apiConfig = this.searchConfig.search;
        }

        return Promise.resolve(this.searchQuery(apiConfig.types[input.type], input))
          .then(query => {
              delete query.search.from;
              delete query.search.size;
              delete query.search.sort;
              return query;
          })
          .then(query => this.esClient.explain(input.id, query))
          .then((response) => response && response.explanation || null);
    }

    explainAutocomplete(headers, input) {
        return this._explain(Constants.AUTOCOMPLETE_API, this.validateInput(input, this.apiSchema.explainAutocomplete));
    }

    explainSearch(headers, input) {
        return this._explain(Constants.SEARCH_API, this.validateInput(input, this.apiSchema.explainSearch));
    }

    termVectors(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.termVectors);

        const typeConfig = this.getIndexTypeConfigFromType(validatedInput.type);

        return Promise.resolve(this.esClient.termVectors(typeConfig.index, typeConfig.type, validatedInput.id))
          .then((response) => response && response.term_vectors || null);
    }

    get(headers, input) {
        // TODO: validate input with schema
        if (!input.id) {
            throw new ValidationError('No ID has been specified', {details: {code: 'UNDEFINED_ID'}});
        }

        if (!input.type) {
            throw new ValidationError('No Type has been specified', {details: {code: 'UNDEFINED_TYPE'}});
        }

        const typeConfig = this.getIndexTypeConfigFromType(input.type);

        return Promise.resolve(this.esClient.get(typeConfig.index, typeConfig.type, input.id))
          .then((response) => response && this._processSource(response, (typeConfig && (typeConfig.name || typeConfig.type)) || input.type) || null);
    }

    // TODO: create schema to validate view input
    view(headers, input) {
        const type = input.type;

        const viewConfig = this.searchConfig.views.types[type];
        const indexTypeConfig = viewConfig.indexType;

        const filter = this.filterQueries(viewConfig, input);
        const postFilters = this.postFilters(viewConfig, input);

        const query = {
            sort: this.sortPart(viewConfig, input) || undefined,
            query: {
                bool: {filter}
            }
        };

        const finalResponse = {
            totalResults: 0,
            results: []
        };

        // console.log('=======> View: ', JSON.stringify(indexTypeConfig));

        return this.esClient.allPages(indexTypeConfig.index, indexTypeConfig.type, query, 100,
          (response) => {
              if (response && response.hits && response.hits.hits) {
                  const hits = response.hits.hits;
                  if (hits) {
                      _.forEach(hits, (hit) => {
                          const doc = hit._source;
                          if (!postFilters || _.every(postFilters, postFilter => postFilter(doc))) {
                              finalResponse.totalResults++;
                              finalResponse.results.push(doc);
                          }
                      });
                  }
              }
          })
          .then(() => finalResponse);
    }
}

export default class Searcher {
    constructor(searchConfig) {
        this.internal = new SearcherInternal(searchConfig);
    }

    errorWrap(method, request, promise) {
        return Promise.resolve(promise)
          .catch(error => {
              console.error('>>> Error', method, request, error, error.stack);

              if (error && (error._errorCode === 'VALIDATION_ERROR' || error._errorCode === 'INTERNAL_SERVICE_ERROR')) {
                  // rethrow same error
                  throw error;
              }

              throw new InternalServiceError('Internal Service Error', {details: error && error.cause || error, stack: error && error.stack});
          });
    }

    get(headers, request) {
        return this.errorWrap('get', request, this.internal.get(headers, request));
    }

    search(headers, request) {
        return this.errorWrap('search', request, this.internal.search(headers, request));
    }

    formSearch(headers, request) {
        return this.errorWrap('formSearch', request, this.internal.formSearch(headers, request));
    }

    browseAll(headers, request) {
        return this.errorWrap('browseAll', request, this.internal.browseAll(headers, request));
    }

    autocomplete(headers, request) {
        return this.errorWrap('autocomplete', request, this.internal.autocomplete(headers, request));
    }

    suggestedQueries(headers, request) {
        return this.errorWrap('suggestedQueries', request, this.internal.suggestedQueries(headers, request));
    }

    explainAutocomplete(headers, request) {
        return this.errorWrap('explainAutocomplete', request, this.internal.explainAutocomplete(headers, request));
    }

    explainSearch(headers, request) {
        return this.errorWrap('explainSearch', request, this.internal.explainSearch(headers, request));
    }

    termVectors(headers, request) {
        return this.errorWrap('termVectors', request, this.internal.termVectors(headers, request));
    }

    didYouMean(headers, request) {
        return this.errorWrap('didYouMean', request, this.internal.didYouMean(headers, request));
    }

    view(headers, request) {
        return this.errorWrap('view', request, this.internal.view(headers, request));
    }

    registry() {
        return {
            autocomplete: [
                {handler: this.autocomplete},
                {handler: this.autocomplete, method: 'get'}
            ],
            search: [
                {handler: this.search},
                {handler: this.search, method: 'get'}
            ],
            formSearch: [
                {handler: this.formSearch},
                {handler: this.formSearch, method: 'get'}
            ],
            browseAll: [
                {handler: this.browseAll},
                {handler: this.browseAll, method: 'get'}
            ],
            suggestedQueries: [
                {handler: this.suggestedQueries},
                {handler: this.suggestedQueries, method: 'get'}
            ],
            didYouMean: [
                // {handler: this.didYouMean},
                {handler: this.didYouMean, method: 'get'}
            ],
            'explain/search': [
                {handler: this.explainSearch},
                {handler: this.explainSearch, method: 'get'}
            ],
            'explain/autocomplete': [
                {handler: this.explainAutocomplete},
                {handler: this.explainAutocomplete, method: 'get'}
            ],
            termVectors: {handler: this.termVectors, method: 'get'},
            view: [
                {handler: this.view},
                {handler: this.view, method: 'get'}
            ],
            ':type/autocomplete': [
                {handler: this.autocomplete},
                {handler: this.autocomplete, method: 'get'}
            ],
            ':type/search': [
                {handler: this.search},
                {handler: this.search, method: 'get'}
            ],
            ':type/formSearch': [
                {handler: this.formSearch},
                {handler: this.formSearch, method: 'get'}
            ],
            ':type/browseAll': [
                {handler: this.browseAll},
                {handler: this.browseAll, method: 'get'}
            ],
            ':type/suggestedQueries': [
                {handler: this.suggestedQueries},
                {handler: this.suggestedQueries, method: 'get'}
            ],
            ':type/didYouMean': [
                // {handler: this.didYouMean},
                {handler: this.didYouMean, method: 'get'}
            ],
            ':type/view': [
                {handler: this.view},
                {handler: this.view, method: 'get'}
            ],
            ':type/:id/termVectors': {handler: this.termVectors, method: 'get'},
            ':type/:id': {handler: this.get, method: 'get'},
            '/': {handler: this.get, method: 'get'}
        };
    }
}