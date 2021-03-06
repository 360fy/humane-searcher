// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import {EventEmitter} from 'events';
import qs from 'qs';
import LanguageDetector from 'humane-node-commons/lib/LanguageDetector';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import InternalServiceError from 'humane-node-commons/lib/InternalServiceError';
import ESClient from './ESClient';
import * as Constants from './Constants';
import buildApiSchema from './ApiSchemaBuilder';
// import SearchEventHandler from './SearchEventHandler';

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
                            field: 'unicodeValue',
                            vernacularOnly: true,
                            weight: 10
                        },
                        {
                            field: 'value',
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

        // const DefaultEventHandlers = {
        //     search: data => new SearchEventHandler(this.instanceName).handle(data)
        // };

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
        // this.registerEventHandlers(config.searchConfig.eventHandlers);
    }

    // eslint-disable-next-line class-methods-use-this
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

    // eslint-disable-next-line class-methods-use-this
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

    // eslint-disable-next-line class-methods-use-this
    constantScoreQuery(fieldConfig, query) {
        if (fieldConfig.filter || (query && (query.humane_query || query.multi_humane_query))) {
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

    // eslint-disable-next-line class-methods-use-this
    humaneQuery(fieldConfig, text, intentFields) {
        return {
            humane_query: {
                [fieldConfig.field]: {
                    instance: this.instanceName, // new
                    // intentIndex: `${_.toLower(this.instanceName)}:intent_store`, // old
                    intentFields,
                    query: text,
                    boost: fieldConfig.weight,
                    vernacularOnly: fieldConfig.vernacularOnly,
                    keyword: fieldConfig.keyword,

                    //path: fieldConfig.nestedPath,
                    noFuzzy: fieldConfig.noFuzzy
                }
            }
        };
    }

    // eslint-disable-next-line class-methods-use-this
    termQuery(fieldConfig, text) {
        const queryType = _.isArray(text) ? 'terms' : 'term';
        return {
            [queryType]: {
                [fieldConfig.field]: text
            }
        };
    }

    // eslint-disable-next-line class-methods-use-this
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

    // eslint-disable-next-line class-methods-use-this
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

    // eslint-disable-next-line class-methods-use-this
    existsQuery(field) {
        return {
            bool: {
                must: {
                    exists: {
                        field
                    }
                }
            }
        };
    }

    buildFieldQuery(fieldConfig, valueOrArrayOfValue, queries, intentFields) {
        let query = null;

        if (fieldConfig.filter && fieldConfig.rangeQuery) {
            if (!_.isArray(valueOrArrayOfValue)) {
                valueOrArrayOfValue = [valueOrArrayOfValue];
            }

            const matchingRangeQueries = [];
            _.forEach(valueOrArrayOfValue, (oneValue) => {
                matchingRangeQueries.push({
                    range: {
                        [fieldConfig.field]: {
                            gte: oneValue.from,
                            lt: oneValue.to
                        }
                    }
                });
            });

            query = this.boolShouldQueries(matchingRangeQueries);
        } else if (fieldConfig.filter && fieldConfig.termQuery) {
            if (valueOrArrayOfValue === '__not_empty__') {
                query = this.existsQuery(fieldConfig.field);
            } else {
                query = this.termQuery(fieldConfig, valueOrArrayOfValue);
            }
        } else {
            query = this.humaneQuery(fieldConfig, valueOrArrayOfValue, intentFields);
        }

        if (query == null) {
            return null;
        }

        query = this.wrapQuery(fieldConfig, query);

        if (fieldConfig.filter && (fieldConfig.termQuery || fieldConfig.rangeQuery) && fieldConfig.includeMissing && valueOrArrayOfValue !== '__not_empty__') {
            query = this.boolShouldQueries([query, this.wrapQuery(fieldConfig, this.missingQuery(fieldConfig.field))]);
        }

        if (queries) {
            if (_.isArray(query)) {
                _.forEach(query, singleQuery => queries.push(singleQuery));
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

    buildTypeQuery(searchTypeConfig, text, fuzzySearch, intentFields) {
        if (!text || _.isEmpty(text)) {
            return {};
        }

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
                            keyword: queryField.keyword,
                            instance: this.instanceName, // new
                            // intentIndex: `${_.toLower(this.instanceName)}:intent_store`, // old
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
                    instance: this.instanceName, // new
                    // intentIndex: `${_.toLower(this.instanceName)}:intent_store`, // old
                    intentFields,
                    fields: _(queryFields)
                      .map(queryField => ({
                          field: queryField.field,
                          boost: queryField.weight,
                          vernacularOnly: queryField.vernacularOnly,
                          keyword: queryField.keyword,
                          path: queryField.nestedPath,
                          noFuzzy: !fuzzySearch || queryField.noFuzzy
                      }))
                      .value()

                }
            }
        };
    }

    // eslint-disable-next-line class-methods-use-this
    isValidValue(value) {
        return !_.isUndefined(value) && !_.isNull(value);
    }

    filterQueries(searchTypeConfig, input, termLanguages, intentFields) {
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
            let range = false;
            if (input.filter && this.isValidValue(input.filter[key])) {
                filterValue = input.filter[key];
            } else if (filterConfig.defaultValue) {
                filterValue = filterConfig.defaultValue;
            }

            if (this.isValidValue(filterValue) && filterValue !== '__all__') {
                let filterType = null;

                if (_.isObject(filterValue)) {
                    filterType = filterValue.type;
                    if (filterValue.value) {
                        filterValue = filterValue.value;
                    } else if (filterValue.values) {
                        filterValue = filterValue.values;
                    } else if (filterValue.range) {
                        filterValue = filterValue.range;
                        range = true;
                    } else if (filterValue.ranges) {
                        filterValue = filterValue.ranges;
                        range = true;
                    }
                }

                if (filterType && filterType === 'facet') {
                    return true;
                }

                if (filterConfig.value && _.isFunction(filterConfig.value)) {
                    filterValue = filterConfig.value(filterValue);
                }

                this.buildFieldQuery(_.defaults({filter: true, rangeQuery: range, termQuery: !range && filterConfig.termQuery}, filterConfig), filterValue, filterQueries, intentFields);
            }

            return true;
        });

        if (input.lang && !_.isEmpty(input.lang)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), input.lang, filterQueries, intentFields);
        }

        if (termLanguages && !_.isEmpty(termLanguages)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), termLanguages, filterQueries, intentFields);
        }

        if (filterQueries.length === 0) {
            return undefined;
        }

        if (filterQueries.length === 1) {
            return filterQueries[0];
        }

        // return _.map(filterQueries, filter => ({query: filter}));

        return filterQueries;
    }

    facetQueries(searchTypeConfig, input, intentFields) {
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

            if (filterValue && filterValue !== '__all__' && _.isObject(filterValue)) {
                const filterType = filterValue.type;
                let range = false;
                if (filterValue.value) {
                    filterValue = filterValue.value;
                } else if (filterValue.values) {
                    filterValue = filterValue.values;
                } else if (filterValue.range) {
                    filterValue = filterValue.range;
                    range = true;
                } else if (filterValue.ranges) {
                    filterValue = filterValue.ranges;
                    range = true;
                }

                if (!filterType || filterType !== 'facet') {
                    return true;
                }

                if (facetConfig.type === 'field' || facetConfig.type === 'min-max') {
                    // form field query here - termQuery, nestedPath, field
                    this.buildFieldQuery({
                        filter: true,
                        termQuery: !range,
                        rangeQuery: range,
                        field: facetConfig.field,
                        nestedPath: facetConfig.nestedPath,
                        includeMissing: facetConfig.includeMissing,
                        supportsRangeQuery: facetConfig.supportsRangeQuery
                    }, filterValue, facetQueries, intentFields);
                } else if (facetConfig.type === 'filters') {
                    // find matching filter and form appropriate query here
                    const matchingFilterQueries = [];
                    if (!_.isArray(filterValue)) {
                        filterValue = [filterValue];
                    }

                    _.forEach(filterValue, (oneValue) => {
                        _.forEach(facetConfig.filters, (filterFacetConfig) => {
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

                    _.forEach(filterValue, (oneValue) => {
                        _.forEach(facetConfig.ranges, (filterRangeConfig) => {
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
                    if (facetConfig.includeMissing) {
                        matchingRangeQueries.push(this.missingQuery(facetConfig.field));
                    }

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

        // return _.map(facetQueries, filter => ({query: filter}));

        return facetQueries;
    }

    // todo: see the usage of it...
    // eslint-disable-next-line class-methods-use-this
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
                if ((_.isString(config) && config === input.sort.field)
                  || (_.isObject(config) && config.field === input.sort.field)) {
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

    // eslint-disable-next-line class-methods-use-this
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
                    size: 1000 // 1000 values are enough here
                }
            };
        } else if (facetConfig.type === 'min-max') {
            facetValue = {
                stats: {
                    field: facetConfig.field
                }
            };
        } else if (facetConfig.type === 'ranges') {
            if (!facetConfig.ranges) {
                throw new ValidationError('No ranges defined for range type facet', {details: {code: 'NO_RANGES_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
            }

            facetValue = {
                range: {
                    field: facetConfig.field,
                    ranges: _.map(facetConfig.ranges, (range) => {
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

            _.forEach(facetConfig.filters, (filter) => {
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

        if ((facetConfig.type === 'field' || facetConfig.type === 'ranges' || facetConfig.type === 'min-max') && facetConfig.nestedPath) {
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

        _.forEach(facetConfigs, (facetConfig) => {
            const facet = this.facet(facetConfig, searchTypeConfig.summaries);
            facets[facet.key] = facet.value;
        });

        return facets;
    }

    // eslint-disable-next-line class-methods-use-this
    _searchQueryInternal(index, query, page, size, queryLanguages, type, sort, facets, postFilter) {
        // const indexTypeConfig = searchTypeConfig.indexType;
        //
        // let sort = this.sortPart(searchTypeConfig, input) || undefined;
        // if (sort && _.isEmpty(sort)) {
        //     sort = undefined;
        // }
        //
        // let facets = this.facetsPart(searchTypeConfig) || undefined;
        // if (facets && _.isEmpty(facets)) {
        //     facets = undefined;
        // }

        return {
            index,
            type,
            search: {
                from: (page || 0) * (size || 0),
                size,
                sort,
                query: {
                    function_score: {
                        query,
                        field_value_factor: {
                            field: '_weight',
                            factor: 2.0,
                            missing: 1
                        }
                    }
                },
                post_filter: postFilter,
                aggs: facets
            },
            queryLanguages
        };
    }

    searchQuery(searchTypeConfig, input, intentFields, query, queryLanguages) {
        const indexTypeConfig = searchTypeConfig.indexType;

        let sort = this.sortPart(searchTypeConfig, input) || undefined;
        if (sort && _.isEmpty(sort)) {
            sort = undefined;
        }

        let facets = this.facetsPart(searchTypeConfig) || undefined;
        if (facets && _.isEmpty(facets)) {
            facets = undefined;
        }

        const filter = this.filterQueries(searchTypeConfig, input, _.keys(queryLanguages), intentFields);

        return this._searchQueryInternal(
          indexTypeConfig.index,
          {
              bool: {
                  must: query || {match_all: {}},
                  filter
              }
          },
          input.page || 0,
          input.count,
          queryLanguages,
          indexTypeConfig.type,
          sort,
          facets,
          this.facetQueries(searchTypeConfig, input, intentFields)
        );
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

                  if ((this.instanceName === 'carDekho' || this.instanceName === 'carDekhoV2') && (key === 'features' || key === 'colors' || key === 'content')) {
                      return true;
                  }

                  return false;
              })
              .mapValues(value => this._deepOmit(value))
              .value();
        }

        return source;
    }

    _processSource(hit/*, name*/) {
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

        return _.defaults(_.pick(hit, ['_id', '_score', '_type', '_weight', '_version']), /*{_name: name},*/ source);
    }

    // eslint-disable-next-line class-methods-use-this
    _baseUrl(input, type, apiType) {
        const inputParams = _(input)
          .pick(['text', 'filter', 'sort'])
          // .mapValues((value, key) => {
          //     if (key === 'type' && (value === '' || value === '*')) {
          //         return undefined;
          //     }
          //
          //     return value;
          // })
          .value();

        inputParams.type = type;

        return `/searcher/api/${apiType}?${qs.stringify(inputParams, {allowDots: true, skipNulls: true})}`;
    }

    _processResponse(response, searchTypesConfig, type, input, apiType) {
        // let type = null;
        // let name = null;

        const results = [];

        if (response.hits && response.hits.hits) {
            // _.forEach(response.hits.hits, (hit) => {
            //     results.push(this._processSource(hit));
            // });

            let previousScore = null;
            _.forEach(response.hits.hits, (hit) => {
                const hitOut = this._processSource(hit);

                if (previousScore == null || (hitOut._score * 1.0) / previousScore > 0.40) {
                    results.push(hitOut);
                    previousScore = hitOut._score;
                }
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
            _.forEach(summaryKeys, (summaryKey) => {
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

            _.forEach(facetConfigs, (facetConfig) => {
                let facet = response.aggregations[facetConfig.key];

                if (!facet) {
                    return true;
                }

                if ((facetConfig.type === 'field' || facetConfig.type === 'ranges' || facetConfig.type === 'min-max') && facetConfig.nestedPath) {
                    facet = facet.nested;
                }

                if (facetConfig.type === 'min-max') {
                    facets[facetConfig.key] = facet;
                } else if (facetConfig.type === 'filter' || facetConfig.type === 'filters') {
                    // bucket is an object with key as object key and doc_count as value
                    const output = facets[facetConfig.key] = [];

                    _.forEach(facet.buckets, (bucket, key) => {
                        const facetResult = {
                            key,
                            count: bucket.doc_count,
                            from: facet.from,
                            from_as_string: facet.from_as_string,
                            to: facet.to,
                            to_as_string: facet.to_as_string
                        };

                        if (summaryKeys) {
                            _.forEach(summaryKeys, (summaryKey) => {
                                facetResult[summaryKey] = _.get(bucket, [summaryKey, 'value']);
                            });
                        }

                        output.push(facetResult);
                    });
                } else {
                    // bucket is an array of objects with key and doc_count
                    facets[facetConfig.key] = _.map(facet.buckets, (bucket) => {
                        const facetResult = {
                            key: bucket.key,
                            count: bucket.doc_count,
                            from: facet.from,
                            from_as_string: facet.from_as_string,
                            to: facet.to,
                            to_as_string: facet.to_as_string
                        };

                        if (summaryKeys) {
                            _.forEach(summaryKeys, (summaryKey) => {
                                facetResult[summaryKey] = _.get(bucket, [summaryKey, 'value']);
                            });
                        }

                        return facetResult;
                    });
                }

                return true;
            });
        }

        let nextPage;
        let prevPage;

        const baseUrl = this._baseUrl(input, type, apiType);

        if (input.page > 0) {
            prevPage = {
                page: input.page - 1,
                url: `${baseUrl}&page=${input.page - 1}`
            };
        }

        const count = results && results.length;
        const totalResults = _.get(response, 'hits.total', 0);

        if ((input.count * input.page) + count < totalResults) {
            nextPage = {
                page: input.page + 1,
                url: `${baseUrl}&page=${input.page + 1}`
            };
        }

        return {
            type,
            /*name,*/
            resultType: type,
            results,
            summaries,
            facets,
            queryTimeTaken: response.took,
            totalResults,
            count,
            prevPage,
            nextPage
        };
    }

    processMultipleSearchResponse(responses, searchTypesConfig, types, input, apiType) {
        if (!responses) {
            return null;
        }

        const mergedResult = {
            multi: true,
            totalResults: 0,
            results: {}, // todo: better pass these as array of results
            searchText: input && input.text,
            filter: input && input.filter,
            sort: input && input.sort,
            page: input && input.page,
            count: 0
        };

        _.forEach(responses.responses, (response, index) => {
            const type = types && _.isArray(types) && types.length > index && types[index];
            const result = this._processResponse(response, searchTypesConfig, type, input, apiType);

            if (!result || !result.type || /*!result.name ||*/ !result.results || result.results.length === 0) {
                return;
            }

            mergedResult.queryTimeTaken = Math.max(mergedResult.queryTimeTaken || 0, result.queryTimeTaken);
            mergedResult.results[result.type] = result;
            mergedResult.totalResults += result.totalResults;
            mergedResult.count += result && result.results.length;
        });

        let nextPage;
        let prevPage;

        const baseUrl = this._baseUrl(input, null, apiType);

        // if (input.text) {
        //     baseUrl = `${baseUrl}&${qs.stringify({text: input.text}, {allowDots: true})}`;
        // }
        //
        // // if (input.type) {
        // //     baseUrl = `${baseUrl}&text=${input.type}`;
        // // }
        //
        // if (input.filter) {
        //     baseUrl = `${baseUrl}&${qs.stringify({filter: input.filter}, {allowDots: true})}`;
        // }
        //
        // if (input.sort) {
        //     baseUrl = `${baseUrl}&${qs.stringify({sort: input.sort}, {allowDots: true})}`;
        // }

        if (input.page > 0) {
            prevPage = {
                page: input.page - 1,
                url: `${baseUrl}&page=${input.page - 1}`
            };
        }

        const count = mergedResult.count;
        const totalResults = mergedResult.totalResults;

        if ((input.count * input.page) + count < totalResults) {
            nextPage = {
                page: input.page + 1,
                url: `${baseUrl}&page=${input.page + 1}`
            };
        }

        mergedResult.prevPage = prevPage;
        mergedResult.nextPage = nextPage;

        return mergedResult;
    }

    processMultipleSearchResponseAsArray(responses, searchTypesConfig, types, input, apiType) {
        if (!responses) {
            return null;
        }

        const mergedResult = {
            multi: true,
            totalResults: 0,
            results: [],
            searchText: input && input.text,
            filter: input && input.filter,
            sort: input && input.sort,
            page: input && input.page,
            count: 0
        };

        _.forEach(responses.responses, (response) => {
            const result = {
                results: [],
                totalResults: 0,
                count: 0
            };

            if (response.hits && response.hits.hits) {
                result.totalResults = _.get(response, 'hits.total', 0);

                let previousScore = null;
                _.forEach(response.hits.hits, (hit) => {
                    const hitOut = this._processSource(hit);

                    if (previousScore == null || (hitOut._score * 1.0) / previousScore > 0.40) {
                        result.results.push(hitOut);
                        previousScore = hitOut._score;
                    }
                });

                result.count = result.results && result.results.length;
            }

            mergedResult.queryTimeTaken = Math.max(mergedResult.queryTimeTaken || 0, result.queryTimeTaken);
            mergedResult.results.push(result);
            mergedResult.totalResults += result.totalResults;
            mergedResult.count += result && result.count;
        });

        let nextPage;
        let prevPage;

        const baseUrl = this._baseUrl(input, null, apiType);

        if (input.page > 0) {
            prevPage = {
                page: input.page - 1,
                url: `${baseUrl}&page=${input.page - 1}`
            };
        }

        const count = mergedResult.count;
        const totalResults = mergedResult.totalResults;

        if ((input.count * input.page) + count < totalResults) {
            nextPage = {
                page: input.page + 1,
                url: `${baseUrl}&page=${input.page + 1}`
            };
        }

        mergedResult.prevPage = prevPage;
        mergedResult.nextPage = nextPage;

        return mergedResult;
    }

    processSingleSearchResponse(response, searchTypesConfig, type, input, apiType) {
        if (!response) {
            return null;
        }

        const finalResponse = this._processResponse(response, searchTypesConfig, type, input, apiType);

        if (input.bareResponse) {
            return finalResponse;
        }

        return _.extend(finalResponse, {
            searchText: input.text,
            filter: input.filter,
            sort: input.sort,
            page: input.page
            // count: finalResponse && finalResponse.results && finalResponse.results.length
        });
    }

    processFlatSearchResponse(response, searchTypesConfig, input, apiType) {
        if (!response) {
            return null;
        }

        const finalResponse = this._processResponse(response, searchTypesConfig, null, input, apiType);

        return _.extend(finalResponse, {
            searchText: input.text,
            filter: input.filter,
            sort: input.sort,
            page: input.page,
            count: finalResponse && finalResponse.results && finalResponse.results.length
        });
    }

    _queryInternal(headers, input, searchApiConfig) {
        let multiSearch = false;
        let flat = false;

        let promise = null;

        const searchTypeConfigs = searchApiConfig.types;

        let typeOrTypesArray = null;

        let responsePostProcessor = null;

        let text = input.text;

        if ((this.instanceName === '1mg' || this.instanceName === 'netmeds') && text) {
            // fix text
            text = _(text)
              .replace(/(^|[\s]|[^0-9]|[^a-z])([0-9]+)[\s]+(mg|mcg|ml|%)/gi, '$1$2$3')
              .replace(/(^|[\s]|[^0-9]|[^a-z])\.([0-9]+)[\s]*(mg|mcg|ml|%)/gi, '$10.$2$3')
              .replace(/([0-9]+)'S$/gi, '$1S')
              .trim();
        } else if (this.instanceName === 'prettySecrets' && text) {
            // eslint-disable-next-line no-regex-spaces, max-len
            text = text.replace(/(PS 0916MWBHPR-0(?:1|2)|(?:PS[0-9A-Z]+(?:-[0-9A-Z]+)?(?: - BASIX| - PURPLE| ANTIQUE ROSE| A| BERRY| BLACK| BLK.GOLD| BLKPINK| BLUE| BROWN| B| CHERRY| CORALFLR| CREAMFLR| DARKGREY| GIRAFFE| HPR04 Lime Navy| IVORY| LEMON| LILAC| NAVY WHITE| PINK HEART| PINK| PNKSNAKE| PURGREY| PURPLE| RED| REDBLACK| SCARLETT| WHITE| YELLOW|,  HPR04 Yellow orange| PS[0-9A-Z]+(?:-[0-9A-Z]+)?)?))(?:[\s]+|$)/gi, match => _.snakeCase(match));
        }

        // return Promise.resolve(this.buildTypeQuery(searchTypeConfig, text, input.fuzzySearch, intentIndex, intentFields))
        //   .then(({query, queryLanguages}) => {

        let intentFields = [];
        if (!input.type || input.type === '*' || _.isArray(input.type)) {
            typeOrTypesArray = [];
            _(searchTypeConfigs)
              .filter((value, key) => !_.isArray(input.type) || _.some(input.type, val => val === key))
              .values()
              .forEach((searchTypeConfig) => {
                  typeOrTypesArray.push(_.get(searchTypeConfig, 'indexType.type'));
                  intentFields = _.concat(intentFields, _.get(searchTypeConfig, 'intentEntities', []));
              });

            intentFields = _.uniq(intentFields);

            responsePostProcessor = searchApiConfig.multiResponsePostProcessor;

            if (searchApiConfig.flat || input.flat) {
                const searchQueries = _(searchTypeConfigs)
                  .filter((value, key) => !_.isArray(input.type) || _.some(input.type, val => val === key))
                  .values()
                  .map(typeConfig =>
                    Promise.resolve(this.buildTypeQuery(typeConfig, text, input.fuzzySearch, intentFields))
                      .then(({query}) => ({
                          bool: {
                              must: [
                                  query,
                                  {
                                      term: {
                                          _type: {
                                              value: typeConfig.indexType.type
                                          }
                                      }
                                  }
                              ]
                          }
                      })))
                  .value();

                multiSearch = false;
                flat = true;

                promise = Promise.all(searchQueries)
                  .then(queries => this._searchQueryInternal(`${_.toLower(this.instanceName)}_store`, {bool: {should: queries}}, input.page, input.count || 10));
            } else {
                const searchQueries = _(searchTypeConfigs)
                  .filter((value, key) => !_.isArray(input.type) || _.some(input.type, val => val === key))
                  .values()
                  .map(typeConfig =>
                    Promise.resolve(this.buildTypeQuery(typeConfig, text, input.fuzzySearch, intentFields))
                      .then(({query, queryLanguages}) => this.searchQuery(typeConfig, input, intentFields, query, queryLanguages)))
                  .value();

                multiSearch = _.isArray(searchQueries) || false;

                promise = Promise.all(searchQueries);
            }
        } else {
            const searchTypeConfig = searchTypeConfigs[input.type];

            if (!searchTypeConfig) {
                throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'SEARCH_CONFIG_NOT_FOUND', type: input.type}});
            }

            responsePostProcessor = searchTypeConfig.responsePostProcessor;

            typeOrTypesArray = searchTypeConfig.indexType && searchTypeConfig.indexType.type;
            intentFields = _.concat(intentFields, _.get(searchTypeConfig, 'intentEntities', []));

            intentFields = _.uniq(intentFields);

            promise = Promise.resolve(this.buildTypeQuery(searchTypeConfig, text, input.fuzzySearch, intentFields))
              .then(({query, queryLanguages}) => this.searchQuery(searchTypeConfig, input, intentFields, query, queryLanguages));

            // promise = this.searchQuery(searchTypeConfig, input, intentIndex, intentFields);
        }

        // let queryLanguages = null;
        // if (multiSearch) {
        //     queryLanguages = _.head(queryOrArray).queryLanguages;
        // } else {
        //     queryLanguages = queryOrArray.queryLanguages;
        // }

        return Promise.resolve(promise)
        // .then((queryOrArray) => {
        //     console.log('input, queryOrArray: ', input, queryOrArray);
        //     return queryOrArray;
        // })
          .then(queryOrArray => ({queryOrArray, multiSearch, flat, typeOrTypesArray, responsePostProcessor}));
    }

    _searchInternal(headers, input, searchApiConfig, eventName, queryResponse) {
        const searchTypeConfigs = searchApiConfig.types;
        let multiSearch = false;
        let flat = false;
        let queryOrArray = null;
        let typeOrTypesArray = null;
        let responsePostProcessor = null;

        return Promise.resolve(queryResponse || this._queryInternal(headers, input, searchApiConfig))
          .then((response) => {
              queryOrArray = response.queryOrArray;
              multiSearch = response.multiSearch;
              flat = response.flat;
              typeOrTypesArray = response.typeOrTypesArray;
              responsePostProcessor = response.responsePostProcessor;

              if (multiSearch) {
                  return this.esClient.multiSearch(queryOrArray);
              }

              return this.esClient.search(queryOrArray);
          })
          .then((response) => {
              if (multiSearch) {
                  return this.processMultipleSearchResponse(response, searchTypeConfigs, typeOrTypesArray, input, eventName);
              }

              if (flat) {
                  return this.processFlatSearchResponse(response, searchTypeConfigs, input, eventName);
              }

              return this.processSingleSearchResponse(response, searchTypeConfigs, typeOrTypesArray, input, eventName);
          })
          .then((response) => {
              this.eventEmitter.emit(eventName, {headers, queryData: input, queryLanguages: null, queryResult: response});

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
        // const intentIndex = `${_.toLower(this.instanceName)}:intent_store`; // old
        const intentIndex = `${_.toLower(this.instanceName)}:metadata_store`; // new
        const query = input.text;

        const searchTypeConfigs = searchApiConfig.types;

        // TODO: find lookup entities
        // TODO: filter and map these with
        let intentFields = [];
        if (!input.type || input.type === '*') {
            _(searchTypeConfigs)
              .values()
              .forEach((searchTypeConfig) => {
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

        // console.log('Intent Query: ', JSON.stringify(intentQuery, null, 2));

        // form the request and get a response
        return Promise.resolve(this.esClient.intent(intentIndex, intentQuery));
    }

    intent(headers, input) {
        // TODO: do validation later
        return this._intentInternal(headers, input, this.searchConfig.search);
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

    // eslint-disable-next-line class-methods-use-this
    buildSearchSections(text, sectionQueries) {
        return Promise.props(sectionQueries)
          .then((response) => {
              // combine all results into one and return sections
              const finalResult = {
                  searchText: text,
                  results: [],
                  totalResults: 0,
                  count: 0
              };

              _.forEach(response, (value, key) => {
                  const section = _.defaults({type: 'section', name: key}, value);

                  if (value.count === 1) {
                      section.mode = 'single';
                  }

                  finalResult.totalResults += _.get(section, 'totalResults', 0);
                  finalResult.count += _.get(section, 'count', 0);
                  finalResult.results.push(section);
              });

              return finalResult;
          });
    }

    // eslint-disable-next-line class-methods-use-this
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
            let minimumShouldMatch = 1;
            if (intentTokenList.length > 2) {
                if (intentTokenList.length <= 4) {
                    minimumShouldMatch = 2;
                } else {
                    minimumShouldMatch = 3;
                }
            }

            return {
                bool: {
                    should: _.map(intentTokenList, intentToken => this.buildIntentTokenQuery(intentToken, weight, field)),
                    minimum_should_match: minimumShouldMatch
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

    intentSuggestionListQuery(intentSuggestionList, type, field) {
        // return Promise.resolve(this.esClient.search({
        //     index: `${_.toLower(this.instanceName)}_store`,
        //     type,
        //     search: {
        //         query: this.buildIntentSuggestionListQuery(intentSuggestionList, field)
        //     }
        // }))
        //   .then(response => this._processResponse(response, this.searchConfig.search.types, type));
        return {
            index: `${_.toLower(this.instanceName)}_store`,
            type,
            search: {
                query: this.buildIntentSuggestionListQuery(intentSuggestionList, field)
            }
        };
    }

    searchCarDekhoBrand(intentSuggestions) {
        return this.intentSuggestionListQuery(intentSuggestions, 'new_car_brand', 'brand');
    }

    searchCarDekhoModel(intentSuggestions) {
        return this.intentSuggestionListQuery(intentSuggestions, 'new_car_model', 'model');
    }

    searchCarDekhoVariant(intentSuggestions) {
        return this.intentSuggestionListQuery(intentSuggestions, 'new_car_variant', 'variant');
    }

    searchUsedCarsByIntentSuggestions(intentSuggestions, field) {
        return this.intentSuggestionListQuery(intentSuggestions, 'used_car', field);
    }

    searchNewsByIntentSuggestions(intentSuggestions, field) {
        return this.intentSuggestionListQuery(intentSuggestions, 'car_news', field);
    }

    searchNewCarDealersByIntentSuggestions(intentSuggestions, field) {
        return this.intentSuggestionListQuery(intentSuggestions, 'new_car_dealer', field);
    }

    // eslint-disable-next-line class-methods-use-this
    searchUsedCarsByMatchingBrands() {

    }

    // eslint-disable-next-line class-methods-use-this
    searchUsedCarsByMatchingModelsOrBrands() {

    }

    // eslint-disable-next-line class-methods-use-this
    searchNewsByMatchingBrands() {

    }

    // eslint-disable-next-line class-methods-use-this
    searchNewsByMatchingModels() {

    }

    // eslint-disable-next-line class-methods-use-this
    searchNewCarDealersByMatchingBrands() {

    }

    // eslint-disable-next-line class-methods-use-this
    buildSection(sectionResponseOrPromise, sectionName, resultType, sectionTitle, sorter) {
        return Promise.resolve(sectionResponseOrPromise)
          .then(response => (sorter && _.defaults({results: sorter(response.results)}, response)) || response)
          .then(response => (_.defaults({type: 'section', name: sectionName, title: sectionTitle, resultType}, response)));
    }

    // eslint-disable-next-line class-methods-use-this
    composeSections(...sections) {
        return Promise.all(sections)
          .then((responses) => {
              let totalResults = 0;
              const results = [];
              _.forEach(responses, (response) => {
                  if (response) {
                      results.push(response);
                      totalResults += _.get(response, 'totalResults', 0);
                  }
              });

              return {
                  results: _.filter(results, result => result.results && result.results.length > 0),
                  totalResults
              };
          });
    }

    _multiSearch(queries, input) {
        return Promise.resolve(this.esClient.multiSearch(queries))
          .then(response => this.processMultipleSearchResponseAsArray(response, this.searchConfig.search.types, _.map(queries, obj => obj.type), input));
    }

    // eslint-disable-next-line class-methods-use-this
    newCarsQuery(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: ['new_car_model', 'new_car_variant'], flat: true}, input), searchApiConfig))
          .then(response => response.queryOrArray);
    }

    // eslint-disable-next-line class-methods-use-this
    usedCarsQuery(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: 'used_car'}, input), searchApiConfig))
          .then(response => response.queryOrArray);
    }

    // eslint-disable-next-line class-methods-use-this
    carNewsQuery(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: 'car_news'}, input), searchApiConfig))
          .then(response => response.queryOrArray);
    }

    // eslint-disable-next-line class-methods-use-this
    // newCarDealersQuery(headers, input, searchApiConfig) {
    //     return Promise.resolve(this._queryInternal(headers, _.defaults({type: 'new_car_dealer'}, input), searchApiConfig))
    //       .then(response => response.queryOrArray);
    // }

    // eslint-disable-next-line class-methods-use-this
    sortNewCarResults(results) {
        // eslint-disable-next-line no-confusing-arrow
        return _.sortBy(results, result => -1.0 * result._score, result => result._type === 'new_car_model' ? 0 : 1);
    }

    // eslint-disable-next-line class-methods-use-this
    searchNewCars(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: ['new_car_model', 'new_car_variant'], flat: true}, input), searchApiConfig))
          .then(response => this._searchInternal(headers, input, searchApiConfig, Constants.SEARCH_EVENT, response))
          .then(response => _.defaults({type: 'new_car', resultType: 'new_car', results: this.sortNewCarResults(response.results)}, response));
    }

    // eslint-disable-next-line class-methods-use-this
    searchUsedCars(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: 'used_car'}, input), searchApiConfig))
          .then(response => this._searchInternal(headers, input, searchApiConfig, Constants.SEARCH_EVENT, response));
    }

    // eslint-disable-next-line class-methods-use-this
    searchCarNews(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: 'car_news'}, input), searchApiConfig))
          .then(response => this._searchInternal(headers, input, searchApiConfig, Constants.SEARCH_EVENT, response));
    }

    // eslint-disable-next-line class-methods-use-this
    searchNewCarDealers(headers, input, searchApiConfig) {
        return Promise.resolve(this._queryInternal(headers, _.defaults({type: 'new_car_dealer'}, input), searchApiConfig))
          .then(response => this._searchInternal(headers, input, searchApiConfig, Constants.SEARCH_EVENT, response));
    }

    searchWithoutIntent(headers, input, searchApiConfig) {
        return this._multiSearch([
            this.newCarsQuery(headers, input, searchApiConfig),
            this.usedCarsQuery(headers, input, searchApiConfig),
            this.carNewsQuery(headers, input, searchApiConfig),
            // this.newCarDealersQuery(headers, input, searchApiConfig),
        ], input)
          .then(multiResponse => this.composeSections(
            this.buildSection(_.get(multiResponse, 'results[0]'), 'new_car', 'new_car', 'New Cars', this.sortNewCarResults),
            this.buildSection(_.get(multiResponse, 'results[1]'), 'used-cars', 'used_car', 'Used Cars'),
            this.buildSection(_.get(multiResponse, 'results[2]'), 'news', 'car_news', 'News')
            // this.buildSection(_.get(multiResponse, 'results[3]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
          ));
    }

    carDekhoIntentBasedSearch(intentSuggestions, headers, input, searchApiConfig) {
        const typeDecipherQueries = [
            this.searchCarDekhoBrand(intentSuggestions),
            this.searchCarDekhoModel(intentSuggestions),
            this.searchCarDekhoVariant(intentSuggestions)
        ];

        return Promise.resolve(this._multiSearch(typeDecipherQueries, input))
          .then((response) => {
              // check if brand result and how many
              // else check if model result and how many
              // else check if variant results and how many
              const brandHits = _.get(response, 'results[0].totalResults', 0);
              const modelHits = _.get(response, 'results[1].totalResults', 0);
              const variantHits = _.get(response, 'results[2].totalResults', 0);

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
                  return this.searchWithoutIntent(headers, input, searchApiConfig);
              }

              if (searchType === 'brand-single') {
                  // model search for brand
                  // used car search for brand
                  // news search for brand
                  // new car dealers for brand

                  return Promise.resolve(this._multiSearch([
                      this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'brand'),
                      this.searchNewsByIntentSuggestions(intentSuggestions, 'brand'),
                      this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand')
                  ], input))
                    .then(sectionResponses => this.composeSections(
                      this.buildSection(_.get(response, 'results[1]'), 'new_car', 'new_car', 'New Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[0]'), 'used-cars', 'used_car', 'Used Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[1]'), 'news', 'car_news', 'News'),
                      this.buildSection(_.get(sectionResponses, 'results[2]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                    ));
              } else if (searchType === 'brand-multi') {
                  // matching brands
                  // used car search for matching brands
                  // news search for matching brands
                  // new car dealers for matching brands

                  return Promise.resolve(this._multiSearch([
                      this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'brand'),
                      this.searchNewsByIntentSuggestions(intentSuggestions, 'brand'),
                      this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand')
                  ], input))
                    .then(sectionResponses => this.composeSections(
                      this.buildSection(_.get(response, 'results[0]'), 'new_car', 'new_car', 'New Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[0]'), 'used-cars', 'used_car', 'Used Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[1]'), 'news', 'car_news', 'News'),
                      this.buildSection(_.get(sectionResponses, 'results[2]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                    ));
              } else if (searchType === 'model-single') {
                  // matching single model
                  // variants for matching model
                  // used cars for matching model or brand of the matching model
                  // news for matching model or brand of the matching model
                  // new car dealers for brand of the matching model

                  return Promise.resolve(this._multiSearch([
                      this.newCarsQuery(headers, input, searchApiConfig),
                      this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'),
                      this.searchNewsByIntentSuggestions(intentSuggestions, 'model'),
                      this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand')
                  ], input))
                    .then(sectionResponses => this.composeSections(
                      this.buildSection(_.get(sectionResponses, 'results[0]'), 'new_car', 'new_car', 'New Cars', this.sortNewCarResults),
                      this.buildSection(_.get(sectionResponses, 'results[1]'), 'used-cars', 'used_car', 'Used Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[2]'), 'news', 'car_news', 'News'),
                      this.buildSection(_.get(sectionResponses, 'results[3]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                    ));
              } else if (searchType === 'model-multi') {
                  // matching models
                  // used cars for matching models or brand of the matching models
                  // news for matching models or brand of the matching models
                  // new car dealers for brand of the matching models

                  return Promise.resolve(this._multiSearch([
                      this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'),
                      this.searchNewsByIntentSuggestions(intentSuggestions, 'model'),
                      this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand')
                  ], input))
                    .then(sectionResponses => this.composeSections(
                      this.buildSection(_.get(response, 'results[1]'), 'new_car', 'new_car', 'New Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[0]'), 'used-cars', 'used_car', 'Used Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[1]'), 'news', 'car_news', 'News'),
                      this.buildSection(_.get(sectionResponses, 'results[2]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                    ));
              } else if (searchType === 'variant-single') {
                  // matching single variant
                  // similar variants
                  // used cars for models of the matching variant
                  // news for models of the matching variant
                  // new car dealers for brand of the matching variant

                  return Promise.resolve(this._multiSearch([
                      this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'),
                      this.searchNewsByIntentSuggestions(intentSuggestions, 'model'),
                      this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand')
                  ], input))
                    .then(sectionResponses => this.composeSections(
                      this.buildSection(_.get(response, 'results[2]'), 'new_car', 'new_car', 'New Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[0]'), 'used-cars', 'used_car', 'Used Cars'),
                      this.buildSection(_.get(sectionResponses, 'results[1]'), 'news', 'car_news', 'News'),
                      this.buildSection(_.get(sectionResponses, 'results[2]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                    ));
              } //else /*if (searchType === 'variant-multi')*/ {
              // matching variants
              // used cars for models of the matching variants
              // news for models of the matching variants
              // new car dealers for brand of the matching variants

              return Promise.resolve(this._multiSearch([
                  this.searchUsedCarsByIntentSuggestions(intentSuggestions, 'model'),
                  this.searchNewsByIntentSuggestions(intentSuggestions, 'model'),
                  this.searchNewCarDealersByIntentSuggestions(intentSuggestions, 'brand')
              ], input))
                .then(sectionResponses => this.composeSections(
                  this.buildSection(_.get(response, 'results[2]'), 'new_car', 'new_car', 'New Cars'),
                  this.buildSection(_.get(sectionResponses, 'results[0]'), 'used-cars', 'used_car', 'Used Cars'),
                  this.buildSection(_.get(sectionResponses, 'results[1]'), 'news', 'car_news', 'News'),
                  this.buildSection(_.get(sectionResponses, 'results[2]'), 'new-car-dealers', 'new_car_dealer', 'New Car Dealers')
                ));
              //}
          });
    }

    search(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.search);

        if ((this.instanceName === 'carDekho' || this.instanceName === 'carDekhoV2') && validatedInput.section === 'new_car_dealer') {
            return this.searchNewCarDealers(headers, validatedInput, this.searchConfig.search);
        } else if ((this.instanceName === 'carDekho' || this.instanceName === 'carDekhoV2') && validatedInput.section === 'car_news') {
            return this.searchCarNews(headers, validatedInput, this.searchConfig.search);
        } else if ((this.instanceName === 'carDekho' || this.instanceName === 'carDekhoV2') && validatedInput.section === 'used_car') {
            return this.searchUsedCars(headers, validatedInput, this.searchConfig.search);
        } else if ((this.instanceName === 'carDekho' || this.instanceName === 'carDekhoV2') && validatedInput.section === 'new_car') {
            return this.searchNewCars(headers, validatedInput, this.searchConfig.search);
        } else if ((this.instanceName === 'carDekho' || this.instanceName === 'carDekhoV2') && (!validatedInput.type || validatedInput.type === '*')) {
            return Promise.resolve(this._intentInternal(headers, validatedInput, this.searchConfig.search))
              .then((response) => {
                  if (_.isEmpty(response.results)) {
                      return this.searchWithoutIntent(headers, validatedInput, this.searchConfig.search);
                  }

                  // get the first result for now
                  const intentResult = _.first(response.results);
                  const intentSuggestions = _.get(intentResult, ['intent_classes', 'car_name']);
                  if (!intentSuggestions || _.isEmpty(intentSuggestions)) {
                      return this.searchWithoutIntent(headers, validatedInput, this.searchConfig.search);
                  }

                  // make a query for brand, model, or variant
                  return this.carDekhoIntentBasedSearch(intentSuggestions, headers, validatedInput, this.searchConfig.search);
              });
        } else if (this.instanceName === 'netmeds' && (!validatedInput.section || validatedInput.section === '*')) {
            // create two queries (a) for prescribed (b) for non-prescribed
            // combine them in sections
            // TODO: convert this to multi query
            return this.composeSections(
              this.buildSection(this._searchInternal(headers, _.defaultsDeep({
                  bareResponse: true,
                  filter: {prescription: true}
              }, validatedInput), this.searchConfig.search, Constants.SEARCH_EVENT), 'prescription', 'product', 'PRESCRIPTION'),
              this.buildSection(this._searchInternal(headers, _.defaultsDeep({
                  bareResponse: true,
                  filter: {prescription: false}
              }, validatedInput), this.searchConfig.search, Constants.SEARCH_EVENT), 'non_prescription', 'product', 'NON PRESCRIPTIONS')
            );
        } else if (this.instanceName === 'netmeds' && validatedInput.section === 'prescription') {
            return this._searchInternal(headers, _.defaultsDeep({filter: {prescription: true}}, validatedInput), this.searchConfig.search, Constants.SEARCH_EVENT);
        } else if (this.instanceName === 'netmeds' && validatedInput.section === 'non_prescription') {
            return this._searchInternal(headers, _.defaultsDeep({filter: {prescription: false}}, validatedInput), this.searchConfig.search, Constants.SEARCH_EVENT);
        }

        return this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.SEARCH_EVENT);
    }

    // eslint-disable-next-line class-methods-use-this
    didYouMean(/*headers, input*/) {
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
          .then((response) => {
              // merge
              if (response.multi) {
                  // calculate scores
                  const relevancyScores = [];
                  _.forEach(response.results, (resultGroup) => {
                      _.forEach(resultGroup.results, (result) => {
                          result._relevancyScore = result._score / (result._weight || 1.0);
                          relevancyScores.push(result._relevancyScore);
                      });
                  });

                  // order scores in descending order
                  relevancyScores.sort((scoreA, scoreB) => scoreB - scoreA);

                  // find deflection point
                  let previousScore = 0;
                  let deflectionScore = 0;
                  _.forEach(relevancyScores, (score) => {
                      if (previousScore && score < 0.5 * previousScore) {
                          deflectionScore = previousScore;
                          return false;
                      }

                      previousScore = score;

                      return true;
                  });

                  // consider items till the deflection point
                  const results = [];
                  _.forEach(response.results, (resultGroup) => {
                      _.forEach(resultGroup.results, (result) => {
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
          .then((query) => {
              delete query.search.from;
              delete query.search.size;
              delete query.search.sort;
              return query;
          })
          .then(query => this.esClient.explain(input.id, query))
          .then(response => (response && response.explanation) || null);
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
          .then(response => (response && response.term_vectors) || null);
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
          .then(response => (response && this._processSource(response, (typeConfig && (typeConfig.name || typeConfig.type)) || input.type)) || null);
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

        return this.esClient.allPages(indexTypeConfig.index, indexTypeConfig.type, query, 100,
          (response) => {
              if (response && response.hits && response.hits.hits) {
                  const hits = response.hits.hits;
                  if (hits) {
                      _.forEach(hits, (hit) => {
                          const doc = hit._source;
                          if (!postFilters || _.every(postFilters, postFilter => postFilter(doc))) {
                              finalResponse.totalResults += 1;
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

    // eslint-disable-next-line class-methods-use-this
    errorWrap(method, request, promise) {
        return Promise.resolve(promise)
          .catch((error) => {
              console.error('>>> Error', method, request, error, error.stack);

              if (error && (error._errorCode === 'VALIDATION_ERROR' || error._errorCode === 'INTERNAL_SERVICE_ERROR')) {
                  // rethrow same error
                  throw error;
              }

              throw new InternalServiceError('Internal Service Error', {details: (error && error.cause) || error, stack: error && error.stack});
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

    intent(headers, request) {
        return this.errorWrap('intent', request, this.internal.intent(headers, request));
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
            intent: [
                {handler: this.intent},
                {handler: this.intent, method: 'get'}
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
            ':type/intent': [
                {handler: this.intent},
                {handler: this.intent, method: 'get'}
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