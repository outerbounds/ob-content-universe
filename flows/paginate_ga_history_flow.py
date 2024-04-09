from metaflow import FlowSpec, step, pypi, pypi_base, secrets


# @schedule(daily=True)
@pypi_base(python="3.11")
class GoogleAnalyticsSnowflakeELT(FlowSpec):

    @pypi(disabled=True)
    @step
    def start(self):
        self.next(self.extract_data)

    @secrets(["ob-google-analytics", "ob-ga-snowflake"])
    @pypi(
        packages={
            "google-oauth2-tool": "0.0.3",
            "google-api-python-client": "2.125.0",
            "pandas": "2.2.1",
            "snowflake-connector-python": "3.7.1",
        }
    )
    @step
    def extract_data(self):
        from google_analytics import extract_data

        self.data = extract_data()
        # TOOD: push to warehouse
        self.next(self.transform_data)

    @step
    def transform_data(self):
        # TODO: run dbt transformation in decorator, produce card
        self.next(self.notify)

    @step
    def notify(self):
        # TODO: send slack notification with link to card
        self.next(self.end)

    @step
    def end(self):
        pass
