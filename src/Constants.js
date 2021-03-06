export const SCORE_SORT_FIELD = 'score';

export const ORGANIC_MODE = 'organic';
export const ENTITY_AUTOCOMPLETE_MODE = 'autocomplete:entity';
export const POPULAR_SEARCH_AUTOCOMPLETE_MODE = 'autocomplete:popular_search';
export const ENTITY_SUGGESTION_MODE = 'suggestion:entity';
export const POPULAR_SEARCH_SUGGESTION_MODE = 'suggestion:popular_search';
export const VALID_MODES = [
    ORGANIC_MODE,
    ENTITY_AUTOCOMPLETE_MODE,
    POPULAR_SEARCH_AUTOCOMPLETE_MODE,
    ENTITY_SUGGESTION_MODE,
    POPULAR_SEARCH_SUGGESTION_MODE
];

export const ASC_SORT_ORDER = 'ASC';
export const DESC_SORT_ORDER = 'DESC';
export const VALID_SORT_ORDERS = [ASC_SORT_ORDER, DESC_SORT_ORDER];

export const SEARCH_API = 'search';
export const AUTOCOMPLETE_API = 'autocomplete';

export const SEARCH_EVENT = 'search';
export const AUTOCOMPLETE_EVENT = 'autocomplete';
export const SUGGESTED_QUERIES_EVENT = 'suggested_queries';
export const FORM_SEARCH_EVENT = 'form_search';
export const BROWSE_ALL_EVENT = 'browse_all';