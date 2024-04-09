import os
import json
import base64
from datetime import datetime

from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from tqdm import tqdm
import pandas as pd

# Constants from your Google cloud project.
GCP_PROJECT_NAME = "ob-google-analytics"
SERVICE_ACCOUNT_NAME = "ob-google-analytics-sa"
SERVICE_ACCOUNT_ID = "ob-google-analytics-sa"
SERVICE_ACCOUNT_EMAIL_ADDR = (
    "ob-google-analytics-sa@valued-vault-419517.iam.gserviceaccount.com"
)

# https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#metrics
GA_METRICS = [
    "activeUsers",
    "averageSessionDuration",
    "engagementRate",
    "bounceRate",
    "sessions",
]

# https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#dimensions
GA_DIMENSIONS = [
    "browser",
    "city",
    "region",
    "country",
    "dateHourMinute",
    "deviceCategory",
    "eventName",
    "fullPageUrl",
    "landingPage",
]

GA_KEY_FILE_LOCATION = "credentials.json"
GA_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA_PROPERTY_ID = "300797527"  # Unique to each GA property. Can be found in GA UI.
GA_REQUEST_LIMIT = 100_000


def store_json_credentials_file(
    encoded_creds: str, out_file: str = "credentials.json"
) -> None:
    res_decoded = json.loads(base64.b64decode(encoded_creds).decode("utf-8"))
    with open(out_file, "w") as f:
        json.dump(res_decoded, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = out_file


def extract_data(ga_start_date: str, ga_end_date: str) -> pd.DataFrame:

    client = (
        BetaAnalyticsDataClient()
    )  # Assumes GOOGLE_APPLICATION_CREDENTIALS env var is set.
    request = RunReportRequest(
        property=f"properties/{GA_PROPERTY_ID}",
        dimensions=[Dimension(name=dimension_name) for dimension_name in GA_DIMENSIONS],
        metrics=[Metric(name=metric_name) for metric_name in GA_METRICS],
        date_ranges=[DateRange(start_date=ga_start_date, end_date=ga_end_date)],
        limit=GA_REQUEST_LIMIT,
    )
    # TODO: Assumption that there are never more than GA_REQUEST_LIMIT rows in the response.
    # If there are, need to adapt this code to use `limit` and `offset` params & paginate requests.
    response = client.run_report(request)

    data = []
    for row in tqdm(response.rows):
        d = dict(zip(GA_DIMENSIONS, [v.value for v in row.dimension_values]))
        d |= dict(zip(GA_METRICS, [v.value for v in row.metric_values]))
        data.append(d)

    df = pd.DataFrame(data)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if ga_start_date.endswith("daysAgo"):
        _start_date = datetime.now() - pd.Timedelta(
            ga_start_date.replace("daysAgo", "days")
        )
    else:
        _start_date = ga_start_date
    if ga_end_date.endswith("daysAgo"):
        _end_data = datetime.now() - pd.Timedelta(
            ga_end_date.replace("daysAgo", "days")
        )
    else:
        _end_data = ga_end_date

    df["api_query_date"] = [now] * df.shape[0]
    df["api_query_start_date"] = [_start_date] * df.shape[0]
    df["api_query_end_date"] = [_end_data] * df.shape[0]

    # update numerical types
    df["activeUsers"] = df["activeUsers"].astype(int)
    df["averageSessionDuration"] = df["averageSessionDuration"].astype(float)
    df["engagementRate"] = df["engagementRate"].astype(float)
    df["bounceRate"] = df["bounceRate"].astype(float)
    df["sessions"] = df["sessions"].astype(int)

    return df
