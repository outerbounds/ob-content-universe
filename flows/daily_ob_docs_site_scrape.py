from metaflow import FlowSpec, step, pypi_base, pypi, secrets, retry, kubernetes

PYPI_PKGS = {
    "requests": "2.31.0",
    "pandas": "2.1.4",
    "tqdm": "4.66.2",
    "beautifulsoup4": "4.12.3",
    "snowflake-connector-python": "3.7.1",
    "snowflake-sqlalchemy": "1.5.1",
}
SECRET_SRCS = ["snowflake-ob-content-universe"]
OB_DOCS_SCRAPE_SNOWFLAKE_BASE_TABLE = "ob_docs_scrape_base_table"
OB_DOCS_SCRAPE_SNOWFLAKE_SCHEMA = "ob_docs_site_schema"
ARGO_EVENT_NAME = "new_ob_docs_scrape_data"


# @schedule(daily=True)
@pypi_base(python="3.11")
class DailyObDocsSiteScrapeLoader(FlowSpec):

    @retry(times=3)
    @secrets(sources=SECRET_SRCS)
    @pypi(packages=PYPI_PKGS)
    @kubernetes
    @step
    def start(self):
        import os
        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine
        from utils.ob_docs_site_tools import get_urls_from_sitemap, extract_data

        df = extract_data(get_urls_from_sitemap())
        engine = create_engine(
            URL(
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                account=os.environ["SNOWFLAKE_ACCOUNT_IDENTIFIER"],
                warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=OB_DOCS_SCRAPE_SNOWFLAKE_SCHEMA,
                role=os.environ["SNOWFLAKE_OB_CONTENT_UNIVERSE_MF_TASK_ROLE"],
            )
        )
        df.to_sql(
            OB_DOCS_SCRAPE_SNOWFLAKE_BASE_TABLE,
            con=engine,
            schema=OB_DOCS_SCRAPE_SNOWFLAKE_SCHEMA,
            if_exists="append",
            index=False,
        )
        self.next(self.end)

    @step
    def end(self):
        from metaflow.integrations import ArgoEvent

        ArgoEvent(name=ARGO_EVENT_NAME).publish()


if __name__ == "__main__":
    DailyObDocsSiteScrapeLoader()
