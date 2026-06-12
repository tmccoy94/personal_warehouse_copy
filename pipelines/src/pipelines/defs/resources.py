import os

import ibm_db
import requests
import urllib3
import dagster as dg
from sqlalchemy import create_engine

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class WarehousePgResource(dg.ConfigurableResource):
    host: str
    port: str
    database: str
    user: str
    password: str

    def get_engine(self):
        schema = os.environ.get("WAREHOUSE_SCHEMA", "dev")
        return create_engine(
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}",
            connect_args={"options": f"-csearch_path={schema}"},
        )


class KeystoneDatabaseResource(dg.ConfigurableResource):
    host: str
    port: str
    user: str
    password: str

    def get_connection(
        self,
        db_name: str | None = None,
        context=None,
        job_type: str = "daily",
    ):
        """Open a connection to a Keystone DB2 database.

        Pass context to enable schedule-aware DB resolution — scheduled runs
        automatically use the correct DB and ignore KEYSTONE_DB_OVERRIDE.
        Manual runs always respect KEYSTONE_DB_OVERRIDE (or fall back to LIVE).

        Args:
            db_name:  Explicit DB override — bypasses all other logic when set.
            context:  Dagster AssetExecutionContext. When provided and the run
                      is scheduled, resolve_db_for_scheduled_run() selects the
                      correct DB automatically.
            job_type: "daily" or "monthly" — controls which DB the schedule
                      self-corrects to. Ignored for manual runs.
        """
        from pipelines.ks_db_connections import get_ks_connection, resolve_db_for_scheduled_run
        if db_name is None and context is not None:
            db_name = resolve_db_for_scheduled_run(context, job_type)
        return get_ks_connection(self, db_name)


class JasperserverReportingApiResource(dg.ConfigurableResource):
    base_url: str
    username: str
    password: str

    def get(self, path: str, params: dict = None) -> requests.Response:
        response = requests.get(
            f"{self.base_url}{path}",
            params=params,
            auth=(self.username, self.password),
            verify=False,
            timeout=60,
        )
        response.raise_for_status()
        return response

    def fetch_report_csv(self, report_path: str, params: dict | None = None) -> str:
        """Fetch a JasperServer report as CSV text.

        report_path: the path shown in JasperServer admin, e.g.
            "organizations/d0001liv/Reports/Teller/Teller_Activity_Report"
        params: report input controls, e.g. {"StartDate": "2026-01-01", "EndDate": "2026-01-31"}

        Returns the raw CSV string — parse with pd.read_csv(StringIO(result)).

        Some JasperServer configs return Excel format regardless of the .csv URL extension.
        When that happens, this method converts the Excel response to CSV automatically
        using pandas + openpyxl, so callers never see the difference.
        """
        try:
            response = self.get(f"/rest_v2/reports/{report_path}.csv", params=params)
        except requests.HTTPError as exc:
            body = exc.response.text[:2000] if exc.response is not None else "(no response body)"
            raise RuntimeError(
                f"JasperServer returned HTTP {exc.response.status_code} for report '{report_path}' "
                f"with params {params}.\n"
                f"Common causes: wrong parameter name, wrong date format (use YYYY-MM-DD), "
                f"or incorrect report path.\n"
                f"JasperServer response: {body}"
            ) from exc

        content_type = response.headers.get("Content-Type", "")

        # JasperServer sometimes returns CSV content with an Excel content-type header.
        # Try UTF-8 decode regardless of content-type — if it decodes cleanly it's CSV.
        try:
            return response.content.decode("utf-8")
        except UnicodeDecodeError:
            pass

        # Content is genuine binary (not decodable as text) — raise with a useful message.
        preview = response.content[:200].decode("utf-8", errors="replace")
        raise RuntimeError(
            f"JasperServer returned binary content for report '{report_path}' "
            f"with params {params}.\n"
            f"Content-Type: '{content_type}'.\n"
            f"Response preview (lossy): {preview}"
        )


class HubspotApiResource(dg.ConfigurableResource):
    access_token: str

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: dict | None = None) -> requests.Response:
        response = requests.get(
            f"https://api.hubapi.com{path}",
            headers=self._headers(),
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        return response

    def paginate(self, path: str, params: dict | None = None, limit: int = 100):
        """Yield individual result dicts across all pages of a HubSpot list endpoint."""
        page_params = {**(params or {}), "limit": limit}
        after = None

        while True:
            if after:
                page_params["after"] = after
            data = self.get(path, params=page_params).json()
            yield from data.get("results", [])
            paging = data.get("paging")
            if not paging or "next" not in paging:
                break
            after = paging["next"]["after"]


hubspot_api = HubspotApiResource(
    access_token=dg.EnvVar("HUBSPOT_ACCESS_TOKEN"),
)


jasperserver_reporting_api = JasperserverReportingApiResource(
    base_url=dg.EnvVar("JASPER_SERVER_URL"),
    username=dg.EnvVar("JASPER_USERNAME"),
    password=dg.EnvVar("JASPER_PASSWORD"),
)


keystone_database = KeystoneDatabaseResource(
    host=dg.EnvVar("SOURCE_DB_HOST"),
    port=dg.EnvVar("SOURCE_DB_PORT"),
    user=dg.EnvVar("SOURCE_DB_USER"),
    password=dg.EnvVar("SOURCE_DB_PASSWORD"),
)


warehouse_pg = WarehousePgResource(
    host=dg.EnvVar("WAREHOUSE_HOST"),
    port=dg.EnvVar("WAREHOUSE_PORT"),
    database=dg.EnvVar("WAREHOUSE_DB"),
    user=dg.EnvVar("WAREHOUSE_USER"),
    password=dg.EnvVar("WAREHOUSE_PASSWORD"),
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "jasperserver_reporting_api": jasperserver_reporting_api,
            "keystone_database": keystone_database,
            "warehouse_pg": warehouse_pg,
            "hubspot_api": hubspot_api,
        }
    )
