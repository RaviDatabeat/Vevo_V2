"""Utilities to run and fetch Google Ad Manager (GAM) saved reports.

Provides a thin client wrapper around the Google Ad Manager `ReportService`
used to run saved queries, poll for completion, and retrieve a download URL.

Usage example:
    from googleads import ad_manager
    from gam_report_pull import GAMReportClient
    client = ad_manager.AdManagerClient.LoadFromStorage('path/to/googleads.yaml')
    gam = GAMReportClient(client)
    saved = gam.get_saved_query(12345)
    job = gam.run_report(saved)
    url = gam.fetch_report_url(job['id'])

Notes:
- `fetch_report_url` is decorated with `retry` from `retry_logic` to retry on failures.
- The caller is responsible for downloading the CSV from the returned URL.
"""

import json
import locale
import logging
import tempfile
import time
from typing import Any

from googleads import ad_manager
import pandas as pd

from retry_logic import retry

logger = logging.getLogger(__name__)

locale.getdefaultlocale = lambda *args: ["en_US", "UTF-8"]


class GAMReportClient:
    """Client for running GAM saved report queries and retrieving results.

    Args:
        ad_manager_client: An authenticated `googleads.ad_manager.AdManagerClient`.
        version: API version string (default "v202508").
    """

    def __init__(
        self, ad_manager_client: ad_manager.AdManagerClient, version: str = "v202508"
    ) -> None:
        self.version = version
        self.ad_manager_client = ad_manager_client

        # Initialize appropriate service.
        self.report_service = self.ad_manager_client.GetService(
            "ReportService", version=self.version
        )

    @classmethod
    def from_yaml_file(cls, yaml_file_path: str, version: str = "v202508"):
        """Create a `GAMReportClient` using a googleads YAML credentials file.

        Args:
            yaml_file_path: Path to the YAML file used by the `googleads` library.
            version: API version string to use for services.

        Returns:
            An initialized `GAMReportClient` instance.
        """
        ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file_path)

        return cls(ad_manager_client, version)

    @classmethod
    def from_service_account_file(
        cls,
        application_name: str,
        network_code: str,
        service_account_path: str,
        version: str = "v202508",
    ):
        """Create a `GAMReportClient` using service account credentials.

        This helper builds a small YAML string suitable for `googleads`'s
        `LoadFromString` when using a service account private key file.

        Args:
                application_name: The application name to include in the config.
                network_code: The network code to target.
                service_account_path: Path to the private key file for the service account.
                version: API version string to use for services.

        Returns:
                An initialized `GAMReportClient` instance.
        """
        yaml_string = f"""
                ad_manager:
                    application_name: {application_name}
                    network_code: {network_code}
                    path_to_private_key_file: {service_account_path}
                """
        logger.info(yaml_string)
        ad_manager_client = ad_manager.AdManagerClient.LoadFromString(yaml_string)

        return cls(ad_manager_client, version)

    @classmethod
    def from_service_account_obj(
        cls,
        application_name: str,
        network_code: str,
        service_account_dict: dict,
        version: str = "v202508",
    ):
        """Create a `GAMReportClient` using service account credentials.

        This helper builds a small YAML string suitable for `googleads`'s
        `LoadFromString` when using a service account private key file.

        Args:
                application_name: The application name to include in the config.
                network_code: The network code to target.
                service_account_dict: Dict for the service account.
                version: API version string to use for services.

        Returns:
                An initialized `GAMReportClient` instance.
        """

        with tempfile.TemporaryDirectory() as temp_dir:
            cred_path = f"{temp_dir}/creds.json"
            with open(cred_path, "w") as f:
                json.dump(service_account_dict, f)

            yaml_string = f"""
                    ad_manager:
                        application_name: {application_name}
                        network_code: {network_code}
                        path_to_private_key_file: {cred_path}
                    """
            logger.info(yaml_string)
            ad_manager_client = ad_manager.AdManagerClient.LoadFromString(yaml_string)

        return cls(ad_manager_client, version)

    def check_all_networks(self):
        """Print basic information about the current authenticated network.

        Uses the `NetworkService` to obtain the current network's metadata and
        prints the network code and display name. This is primarily a helper
        for manual verification of credentials and the targeted network.
        """
        network_service = self.ad_manager_client.GetService(
            "NetworkService", version=self.version
        )
        networks = network_service.getAllNetworks()
        # client_RON = current_network['effectiveRootAdUnitId']

        for current_network in networks:
            logger.info(
                "Current network has network code '%s' and display name '%s'."
                % (current_network["networkCode"], current_network["displayName"])
            )

    def check_client_service(self):
        """Print basic information about the current authenticated network.

        Uses the `NetworkService` to obtain the current network's metadata and
        prints the network code and display name. This is primarily a helper
        for manual verification of credentials and the targeted network.
        """
        network_service = self.ad_manager_client.GetService(
            "NetworkService", version=self.version
        )
        current_network = network_service.getCurrentNetwork()
        # client_RON = current_network['effectiveRootAdUnitId']

        logger.info(
            "Current network has network code '%s' and display name '%s'."
            % (current_network["networkCode"], current_network["displayName"])
        )

    def get_all_saved_reports(self):
        """Return all saved report queries for the authenticated network.

        Returns:
            The raw API response from `getSavedQueriesByStatement`, typically a
            dictionary that may contain a `results` list of saved query objects.
        """
        # Create statement object to filter for an order.
        statement = ad_manager.StatementBuilder(version=self.version)

        response = self.report_service.getSavedQueriesByStatement(
            statement.ToStatement()
        )

        return response

    def get_saved_query(self, saved_query_id: int):
        """Fetch a single saved query by its numeric ID.

        Args:
            saved_query_id: The numeric ID of the saved report query.

        Returns:
            A dictionary representing the saved query.

        Raises:
            KeyError or IndexError: If the API response contains no `results`.
        """
        # Create statement object to filter for an order.
        statement = (
            ad_manager.StatementBuilder(version=self.version)
            .Where("id = :id")
            .WithBindVariable("id", int(saved_query_id))
            .Limit(1)
        )

        response = self.report_service.getSavedQueriesByStatement(
            statement.ToStatement()
        )

        return response["results"][0]

    def run_report(self, saved_query: Any):
        """Start a report job using a saved query definition.

        Args:
            saved_query: A saved query object (as returned by `get_saved_query`).

        Returns:
            The report job object returned by `runReportJob` (contains job id).
        """
        report_job = {}

        report_job["reportQuery"] = saved_query["reportQuery"]

        report_job_response = self.report_service.runReportJob(report_job)

        return report_job_response

    @retry(retries=3, delay=5)
    def fetch_report_url(self, report_job_id: int, wait_for: int = 30):
        """Poll a report job until completion and return a download URL.

        This method periodically polls `getReportJobStatus` until the job
        is no longer `IN_PROGRESS`. When the job reaches `COMPLETED`, it
        requests a download URL with CSV export options and returns it.

        The function is decorated with `retry` to allow transient failures to
        be retried according to the configured retries/delay.

        Args:
            report_job_id: The numeric report job id returned by `runReportJob`.

        Returns:
            A string URL for downloading the CSV when the job completes, or
            `None` if the job fails.
        """
        # Poll the status of the report job until it is completed
        status = "IN_PROGRESS"
        while status == "IN_PROGRESS":
            report_job_status = self.report_service.getReportJobStatus(report_job_id)
            logger.info(f"{report_job_id} Report job status: {report_job_status}")
            status = report_job_status
            if status == "IN_PROGRESS":
                time.sleep(wait_for)  # Wait 30 seconds before checking again

        # Download the report if it is completed
        if status == "COMPLETED":
            download_url = self.report_service.getReportDownloadUrlWithOptions(
                report_job_id, {"exportFormat": "CSV_DUMP"}
            )
            logger.info(f"{report_job_id} Report is ready.")
            return download_url
        else:
            logger.info(f"{report_job_id} Report job failed.")

    def fetch_report_df(self, report_job_id: int):
        report_download_url = self.fetch_report_url(report_job_id)
        logger.info(report_download_url)
        delivery_df = pd.read_csv(
            report_download_url,  # type: ignore
            compression="gzip",
            low_memory=False,
        )

        renamed_dict = {
            i: i.replace(" ", "_")
            .replace("[", "_")
            .replace("]", "_")
            .lower()
            .split(".")[-1]
            for i in delivery_df.columns
        }
        delivery_df.rename(columns=renamed_dict, inplace=True)

        return delivery_df