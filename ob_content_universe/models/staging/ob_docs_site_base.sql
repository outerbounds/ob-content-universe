select
    "URL" as FULL_PAGE_URL,
    TITLE,
    "DESCRIPTION",
    SCRAPE_DATE,
    "DATE" as PUBLISHED_DATE,
    TAGS,
    AUTHOR,
    CONTENT,
    CANONICAL,
    ALTERNATE
from {{ source('ob_docs_site', 'ob_docs_scrape_base_table') }}