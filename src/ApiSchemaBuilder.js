import _ from 'lodash';
import Joi from 'joi';
import * as Constants from './Constants';

// this function builds API schema as per search config
export default function (searchConfig) {
    const baseSchema = {
        requestTime: Joi.number(),
        count: Joi.number().default(10).optional(),
        page: Joi.number().default(0).optional(),
        format: Joi.string()
          .default('default')
          .valid(['default', 'custom'])
          .optional(),
        __instanceName__: Joi.string().default('default')
    };

    //categories: Joi.array().items(Joi.string()).allow(null).optional()
    //.when('mode', {is: AUTOCOMPLETE_MODE, then: Joi.required(), otherwise: Joi.optional()})
    //  .when('mode', {is: SEARCH_RESULT_MODE, then: Joi.required(), otherwise: Joi.optional()}),
    const searchSchema = _.extend({}, baseSchema, {
        mode: Joi.string()
          .valid(Constants.VALID_MODES)
          .default(Constants.ORGANIC_MODE)
          .allow(null),
        lang: Joi.string().allow(null),
        text: Joi.string().min(1).required(),
        fuzzySearch: Joi.boolean().default(true).optional(),
        type: Joi.string()
          .valid(_.keys(searchConfig.search.types))
          .default(searchConfig.search.defaultType)
          .allow([null, '*']),
        sort: Joi.object()
          .keys({
              field: Joi.string()
              //.valid([Constants.SCORE_SORT_FIELD]) // todo: this is contextual to user and type
                .default(Constants.SCORE_SORT_FIELD),
              order: Joi.string().valid(Constants.VALID_SORT_ORDERS).default(Constants.DESC_SORT_ORDER)
          }),
        filter: Joi.object()// todo: these are contextual to user and type and autocomplete vs search
          .keys({
              lang: Joi.object()
                .keys({
                    primary: Joi.string().required(),
                    secondary: Joi.array()
                      .items(Joi.string())
                      .allow(null)
                      .optional()
                })
                .optional()
          })
          .unknown(true)
          .optional(),
        unicodeText: Joi.string().allow([null, '']).optional(),
        originalInput: Joi.string().min(1).allow(null),
        section: Joi.string()
          .allow([null, '*'])
          .default('*')
          .optional()
    });

    const formSearchSchema = _.extend({}, baseSchema, {
        type: Joi.string()
          .valid(_.keys(searchConfig.search.types))
          .default(searchConfig.search.defaultType)
          .allow([null, '*']),
        sort: Joi.object()
          .keys({
              field: Joi.string()
              //.valid([Constants.SCORE_SORT_FIELD]) // todo: this is contextual to user and type
                .default(Constants.SCORE_SORT_FIELD),
              order: Joi.string().valid(Constants.VALID_SORT_ORDERS).default(Constants.DESC_SORT_ORDER)
          }),
        filter: Joi.object()// todo: these are contextual to user and type and autocomplete vs search
          .keys({
              lang: Joi.object()
                .keys({
                    primary: Joi.string().required(),
                    secondary: Joi.array()
                      .items(Joi.string())
                      .allow(null)
                      .optional()
                })
                .optional()
          })
          .unknown(true)
          .optional()
    });

    const browseAllSchema = _.extend({}, baseSchema, {
        type: Joi.string()
          .valid(_.keys(searchConfig.search.types))
          .default(searchConfig.search.defaultType)
          .allow([null, '*']),
        sort: Joi.object()
          .keys({
              field: Joi.string()
              //.valid([Constants.SCORE_SORT_FIELD]) // todo: this is contextual to user and type
                .default(Constants.SCORE_SORT_FIELD),
              order: Joi.string().valid(Constants.VALID_SORT_ORDERS).default(Constants.DESC_SORT_ORDER)
          })
    });

    const autocompleteSchema = _.extend({}, baseSchema, {
        text: Joi.string().min(1).required(),
        fuzzySearch: Joi.boolean().default(true).optional(),
        type: Joi.string()
          .valid(_.keys(searchConfig.autocomplete.types))
          .default(searchConfig.autocomplete.defaultType)
          .allow([null, '*']),
        filter: Joi.object()// todo: these are contextual to user and type and autocomplete vs search
          .keys({
              lang: Joi.object()
                .keys({
                    primary: Joi.string().required(),
                    secondary: Joi.array()
                      .items(Joi.string())
                      .allow(null)
                      .optional()
                })
                .optional()
          })
          .unknown(true)
          .optional(),
        count: Joi.number().default(5)
    });

    const termVectorsSchema = {
        requestTime: Joi.number(),
        type: Joi.string().valid(_.keys(searchConfig.types)).required(),
        id: Joi.string().required(),
        __instanceName__: Joi.string().default('default')
    };

    const didYouMeanSchema = {
        requestTime: Joi.number(),
        type: Joi.string()
          .valid(_.keys(searchConfig.autocomplete.types))
          .default(searchConfig.autocomplete.defaultType)
          .allow([null, '*']),
        text: Joi.string().min(1).required(),
        __instanceName__: Joi.string().default('default')
    };

    return {
        search: Joi.object().keys(searchSchema),
        autocomplete: Joi.object().keys(autocompleteSchema),
        formSearch: Joi.object().keys(formSearchSchema),
        browseAll: Joi.object().keys(browseAllSchema),
        explainSearch: Joi.object().keys(_.omit(_.extend({}, searchSchema, {id: Joi.string().required()}), ['page', 'count'])),
        explainAutocomplete: Joi.object().keys(_.omit(_.extend({}, autocompleteSchema, {id: Joi.string().required()}), ['page', 'count'])),
        termVectors: Joi.object().keys(termVectorsSchema),
        didYouMean: Joi.object().keys(didYouMeanSchema)
    };
}