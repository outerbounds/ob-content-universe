select 
    ga_base.FULL_PAGE_URL,
    ga_base.API_QUERY_START_DATE as ga_query_start_date,
    ga_base.API_QUERY_END_DATE as ga_query_end_date,
    ob_docs_site_base.CONTENT as content,
    ob_docs_site_base.TITLE as title,
    ob_docs_site_base.SCRAPE_DATE as scrape_date,
    ob_docs_site_base.PUBLISHED_DATE as published_date,
    AVG(AVERAGE_SESSION_DURATION) as AVERAGE_SESSION_DURATION,
    STDDEV(AVERAGE_SESSION_DURATION) as STDDEV_AVERAGE_SESSION_DURATION,
    MAX(AVERAGE_SESSION_DURATION) as MAX_AVERAGE_SESSION_DURATION,
    AVG(ENGAGEMENT_RATE) as AVG_ENGAGEMENT_RATE,
    AVG(BOUNCE_RATE) as AVG_BOUNCE_RATE,
    SUM("SESSIONS") as SUM_SESSIONS
from {{ ref('ga_base') }} as ga_base
join {{ ref('ob_docs_site_base') }} as ob_docs_site_base
on ga_base.FULL_PAGE_URL = ob_docs_site_base.FULL_PAGE_URL
where ga_base.EVENT_NAME = 'page_view'
group by 
    ga_base.FULL_PAGE_URL, 
    content, 
    title, 
    scrape_date, 
    published_date, 
    ga_query_start_date, 
    ga_query_end_date