select
    FULL_PAGE_URL,
    LANDING_PAGE,
    AVG(AVERAGE_SESSION_DURATION) as AVERAGE_SESSION_DURATION,
    STDDEV(AVERAGE_SESSION_DURATION) as STDDEV_AVERAGE_SESSION_DURATION,
    MAX(AVERAGE_SESSION_DURATION) as MAX_AVERAGE_SESSION_DURATION,
    AVG(ENGAGEMENT_RATE) as AVG_ENGAGEMENT_RATE,
    AVG(BOUNCE_RATE) as AVG_BOUNCE_RATE,
    SUM("SESSIONS") as TOTAL_SESSIONS,
from {{ ref('ga_base') }}
where EVENT_NAME = 'request_demo_submissions'
group by FULL_PAGE_URL, LANDING_PAGE, EVENT_NAME