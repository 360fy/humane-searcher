# humane-searcher

## APIs

> Note:
>    * Searcher APIs are available at: `http://<server-url>/:instanceName/searcher/api`.
>    * All types must be valid index types defined in configuration.
>    * BODY must be valid `JSON`.
>    * All requests shall have `Content-Type` header as: `Content-Type: application/json`

### Search for Type

This method build search results for a given type, based on input text.

###### **Method 1**

- TYPE  : `POST`

- URL   : `/search`

- BODY  : A `JSON` object comprising of following fields -

    - **type**: 
        * type of document you want to search [defaults to as defined in search configuration]
        * `null` or `*` means search all types and is called `multi` (response structure would change a bit for this case).
        * must be one of the valid type defined in search configuration
            
    - **mode**: 
        * search mode [defaults to `organic`]
        * modes do not carry any meaning to search, they are useful for categorising type of searches and thus search analytics.
        * valid search modes:
            * `organic` - user for manual input searches
            * `autocomplete:entity` - use for autocompleted entity based search
            * `autocomplete:popular_search` - use for autocompleted popular search query based search
            * `suggestion:entity` - use for suggested entity based search
            * `suggestion:popular_search` - use for suggested popular search based search
                
    - **text**: the input text [mandatory, can not be empty string]
            
    - **originalInput**: 
        * typically for autocomplete selection, searched text would different from original input text.
        * for example: person may have entered just `ss` and selected `ssc` from autocomplete box for search.
        * you can capture such original input here, useful for search analytics.
                      
    - **filter**: 
        * custom filters if any
        * valid filters are the one defined in search configuration
        * Filter structure: `{<filter-name>: <filter-value>}`
        * for `lang` filter only, `filter-value` must be a `JSON` object: 
            * `{primary: <primary-language-code>, secondary: [<secondary codes>]}`
                      
    - **sort**: 
        * a custom sort if any [defaults to `SCORE` based `DESC` sort]
        * Valid structure: `{field: <sort field>, order: <sort order>}`
        * sort field should be a valid sort field defined in search configuration
        * sort order should be one of `DESC` or `ASC` [defaults to `DESC`]
                      
    - **page**: 0 based page number, results are paginated in page size of `count` [defaults to `0`]
    
    - **count**: number of results or page size [defaults to `10`]
    
    - **requestTime**: optional requestTime that can be passed from client to calculate request RTT.
     
    Body may look like following -  
    
    ```
        {
            page: <page-num>, 
            count: <page-size>,
            type: <type>,
            mode: <search-mode>,
            filter: <filters>,
            sort: {field: <sort field>, order: <sort order>},
            requestTime: <request-time-in-epoch>,
            text: <search-text>,
            originalInput: <original input>
        }
    ```

- SUCCESS RESPONSE CODE: `200`

- SUCCESS RESPONSE :

    - `single` type scenario
    
        ```
            {
                totalResults: <num total results>,
                results: [
                    {
                        _id: <id of document>,
                        _score: <relevancy score of document>,
                        _type: <type of document>,
                        weight: <weight of document>,
                        //
                        // other source fields of document
                        //
                    },
                    ...
                ],
                queryTimeTaken: <query time taken in ms>,
                requestTime: <request time as passed in request>,
                serviceTimeTaken: <service time taken in ms>
            }
        ```
    
    - `multi` types scenario
    
        ```
            {
                multi: true,
                totalResults: <num total results for all types>,
                results: {
                    <type>: {
                        results: [
                            {
                                _id: <id of document>,
                                _score: <relevancy score of document>,
                                _type: <type of document>,
                                weight: <weight of document>,
                                //
                                // other source fields of document
                                //
                            },
                            ...
                        ],
                        totalResults: <num total results for the type>
                    }
                },
                queryTimeTaken: <query time taken in ms>,
                requestTime: <request time as passed in request>,
                serviceTimeTaken: <service time taken in ms>
            }
        ```

- ERROR RESPONSE: See Common Error Scenarios
         
###### **Method 2**

- TYPE: `GET`

- URL: `/search`

- PARAMS: [qs](https://github.com/ljharb/qs) equivalent stringify of BODY as in method 1.

- SUCCESS RESPONSE: Same as method 1

- ERROR RESPONSE: Same as method 1

###### **Method 3**

- TYPE: `POST`

- URL: `/:type/search`

- BODY: Same as method-1, but omit `type` in body. 

- SUCCESS RESPONSE: Same as method 1

- ERROR RESPONSE: See Common Error Scenarios

- Note: Not a valid method for multi search

###### **Method 4**

- TYPE: `GET`

- URL: `/:type/search`

- PARAMS: Same as method 2, but omit `type` in params.

- SUCCESS RESPONSE: Same as method 1

- ERROR RESPONSE: See Common Error Scenarios

- Note: Not a valid method for multi search

### Autocomplete for Type

This method builds autocomplete suggestions for a given type, based on input text.

###### **Method 1**

- TYPE  : `POST`

- URL   : `/autocomplete`

- BODY  : A `JSON` object comprising of following fields -

    - **type**: see search for explanation, except valid types are as defined in autocomplete configuration.
            
    - **text**: see search for explanation
            
    - **filter**: see search for explanation
                      
    - **page**: see search for explanation, though pagination does not make sense for autocomplete, but you can still do it if you want.
    
    - **count**: number of results or page size [defaults to `5`]
    
    - **requestTime**: see search for explanation
     
- SUCCESS RESPONSE : see search for explanation and scenarios

- ERROR RESPONSE: See Common Error Scenarios

###### **Method 2**

- TYPE: `GET`

- URL: `/autocomplete`

- PARAMS: [qs](https://github.com/ljharb/qs) equivalent stringify of BODY as in method 1.

- SUCCESS RESPONSE: Same as method 1

- ERROR RESPONSE: See Common Error Scenarios

###### **Method 3**

- TYPE: `POST`

- URL: `/:type/autocomplete`

- BODY: Same as method-1, but omit `type` in body. 

- SUCCESS RESPONSE: Same as method 1

- ERROR RESPONSE: See Common Error Scenarios

- Note: Not a valid method for multi autocomplete

###### **Method 4**

- TYPE: `GET`

- URL: `/:type/autocomplete`

- PARAMS: Same as method 2, but omit `type` in params.

- SUCCESS RESPONSE: Same as method 1

- ERROR RESPONSE: See Common Error Scenarios

- Note: Not a valid method for multi autocomplete
    
### Common Error Scenarios

- Case: Unrecognized Type - when type is not among the configured

    - Http Status Code: 400
    
    - Sample Response Body :
    
      ```json
       {
         "_statusCode": 400,
         "_errorCode": "VALIDATION_ERROR",
         "_status": "ERROR",
         "details": {
           "message": "\"type\" must be one of [category_entity, author_entity, publisher_entity, book, null, *]",
           "path": "type",
           "type": "any.allowOnly",
           "context": {
             "valids": [
               "category_entity",
               "author_entity",
               "publisher_entity",
               "book",
               null,
               "*"
             ],
             "key": "type"
           }
         },
         "_errorId": 1458927217570
       }
       ```
         
- Case: Internal Service Error - when there is some internal service error

    - Http Status Code: 500
    
    - Sample Response Body :
    
      ```json
      {
        "_statusCode": 500,
        "_errorCode": "INTERNAL_SERVICE_ERROR",
        "_status": "ERROR",
        "_errorId": 1458819775194
      }
      ```    
