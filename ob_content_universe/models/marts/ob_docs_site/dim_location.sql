select
    DATE_HOUR_MINUTE,
    CITY,
    REGION,
    COUNTRY,
    EVENT_NAME,
    AVERAGE_SESSION_DURATION,
    ENGAGEMENT_RATE,
    BOUNCE_RATE,
    "SESSIONS",
from {{ ref('ga_base') }}