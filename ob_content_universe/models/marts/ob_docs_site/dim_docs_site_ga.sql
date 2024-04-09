select 
    ga_base.FULL_PAGE_URL,
    ga_base.LANDING_PAGE,
    AVG(AVERAGE_SESSION_DURATION) as AVERAGE_SESSION_DURATION,
    STDDEV(AVERAGE_SESSION_DURATION) as STDDEV_AVERAGE_SESSION_DURATION,
    MAX(AVERAGE_SESSION_DURATION) as MAX_AVERAGE_SESSION_DURATION,
    AVG(ENGAGEMENT_RATE) as AVG_ENGAGEMENT_RATE,
    AVG(BOUNCE_RATE) as AVG_BOUNCE_RATE,
    SUM("SESSIONS") as TOTAL_SESSIONS
from {{ ref('ga_base') }} as ga_base
join {{ ref('ob_docs_site_base') }} as ob_docs_site_base
on ga_base.FULL_PAGE_URL = ob_docs_site_base.FULL_PAGE_URL
where ga_base.EVENT_NAME = 'page_view'
group by ga_base.FULL_PAGE_URL, ga_base.LANDING_PAGE, ga_base.EVENT_NAME