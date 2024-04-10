from metaflow import FlowSpec, step, pypi, pypi_base, secrets, retry, kubernetes

GA_START_DATE, GA_END_DATE = "2daysAgo", "yesterday"
GA_SNOWFLAKE_BASE_TABLE = "ga_base_table"
GA_SNOWFLAKE_SCHEMA = "ob_docs_site_schema"
SECRET_SRCS = ["ob-ga-sa-creds", "snowflake-ob-content-universe"]
PYPI_PKGS = {
    "google-analytics-data": "0.18.7",
    "pandas": "2.1.4",
    "snowflake-connector-python": "3.7.1",
    "snowflake-sqlalchemy": "1.5.1",
    "tqdm": "4.66.2",
}
ARGO_EVENT_NAME = "new_ga_data"


# @schedule(daily=True)
@pypi_base(python="3.11")
class GoogleAnalyticsSnowflakeLoader(FlowSpec):

    @retry(times=3)
    @secrets(sources=SECRET_SRCS)
    @pypi(packages=PYPI_PKGS)
    @kubernetes
    @step
    def start(self):
        import os
        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine
        from utils.ga_tools import extract_data, store_json_credentials_file

        store_json_credentials_file(
            encoded_creds=os.environ["GA_SERVICE_ACCOUNT_CREDS"]
        )
        # Extract raw data within desired time range from google analytics.
        # Time range is intended to be fixed in this workflow.
        # TODO: If instead, you want to make time window dynamic, look at paginate_ga_history_flow.py.
        df = extract_data(GA_START_DATE, GA_END_DATE)
        engine = create_engine(
            URL(
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                account=os.environ["SNOWFLAKE_ACCOUNT_IDENTIFIER"],
                warehouse="google_analytics_wh",
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=GA_SNOWFLAKE_SCHEMA,
                role=os.environ["SNOWFLAKE_OB_CONTENT_UNIVERSE_MF_TASK_ROLE"],
            )
        )
        df.to_sql(
            GA_SNOWFLAKE_BASE_TABLE,
            con=engine,
            schema=GA_SNOWFLAKE_SCHEMA,
            if_exists="append",
            index=False,
        )
        self.next(self.end)

    @pypi(disabled=True)
    @step
    def end(self):
        from metaflow.integrations import ArgoEvent

        ArgoEvent(name=ARGO_EVENT_NAME).publish()


if __name__ == "__main__":
    GoogleAnalyticsSnowflakeLoader()
