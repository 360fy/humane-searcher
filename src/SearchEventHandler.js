// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import * as Request from 'humane-node-commons/lib/Request';
import md5 from 'md5';

export default class SearchEventHandler {
    constructor(instanceName) {
        const url = `http://localhost:3000/${instanceName}/indexer/api`;

        // this.request = Promise.promisify(Request.defaults({
        //     json: true,
        //     agent: keepAliveAgent,
        //     baseUrl: url,
        //     gzip: true,
        //     method: 'POST',
        //     uri: '/searchQuery'
        // }));

        this.request = Request.builder({baseUrl: url, method: 'POST', uri: '/upsert'});
    }

    send(data) {
        this.request({body: {type: 'searchQuery', doc: data, signal: {name: 'hit'}}})
          .then(response => Request.handleResponse(response))
          // .then(response => {
          //     if (_.isArray(response)) {
          //         response = response[0];
          //     }
          //
          //     const result = response.statusCode === 200 ? response.body : null;
          //     if (!result) {
          //         console.warn('Error while sending search query: ', response.statusCode, response.body);
          //     }
          // })
          .catch((error) => {
              console.warn('Error while sending search query: ', error);
          });
    }

    handle(data) {
        const {queryData, queryResult} = data;
        
        let queryLanguages = data.queryLanguages;

        const queryTime = Date.now();
        const hasResults = (queryResult && queryResult.totalResults) || false;
        const query = _.lowerCase(queryData.text);
        let unicodeQuery = null;
        
        if (!queryLanguages) {
            queryLanguages = [];
        }

        if (!_.isEmpty(queryLanguages)) {
            unicodeQuery = query;
        }
        
        let primaryLanguage = [];
        let secondaryLanguages = [];
        if (queryData.filter && queryData.filter.lang) {
            primaryLanguage = queryData.filter.lang.primary || primaryLanguage;
            secondaryLanguages = queryData.filter.lang.secondary || secondaryLanguages;
        }

        let languages = _.union(queryLanguages, primaryLanguage, secondaryLanguages);
        if (_.isEmpty(languages)) {
            languages = ['en'];
        }

        _.forEach(languages, (lang) => {
            const key = md5(`${lang}/${query}`);

            this.send({key, query, unicodeQuery, queryTime, hasResults, lang});
        });
    }
}