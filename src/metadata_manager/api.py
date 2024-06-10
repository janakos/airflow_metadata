import logging
from abc import abstractmethod
from typing import Dict

import google.auth
import requests
from google.cloud import secretmanager


class ApiManager:
    """
    This is a parent class to all metadata manager subclasses. This parent class is helpful because we can define
    abstract classes that all metadata managers should implement, and allows us to implement methods that are agnostic
    to metadata types, like `delete_metadata`.
    """

    __AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
    __CREDENTIALS, _ = google.auth.default(scopes=[__AUTH_SCOPE])
    __DEFAULT_TIMEOUT_SECONDS = 720

    def __init__(self, **kwargs):
        self.session = requests.Session()
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/iguana-insights-staging/secrets/gh_service_account_airflow_password/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        payload = response.payload.data.decode("UTF-8")
        self.session.auth = ("gh-service-account", payload)
        self.dag_manifest = kwargs.get("dag_manifest", None)
        self.pause_all = kwargs.get("pause_all", False)
        self.webserver_url = f'{kwargs["webserver_url"]}/api/v1'

    def _request(
        self, request_url: str, method: str = "GET", **kwargs: Dict[str, str]
    ) -> google.auth.transport.Response:
        """
        Make a request to a Cloud Composer 2 environment web server.
        Args:
          url: The URL to fetch.
          method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
            'PATCH', 'DELETE')
          **kwargs: Any of the parameters defined for the request function:
                    https://github.com/requests/requests/blob/master/requests/api.py
                      If no timeout is provided, it is set to 90 by default.
        """
        # Set the default timeout, if missing
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.__DEFAULT_TIMEOUT_SECONDS
        logging.debug(request_url)
        return self.session.request(method, request_url, **kwargs)

    def get_secret(self, secret_name: str) -> str:
        """
        Retrieve a secret from Google Secret manager from the current project ID.
        Args:
            secret_name: The name of the secret we want to retrieve.

        Returns:
            The decoded secret value as a string.
        """
        client = secretmanager.SecretManagerServiceClient()
        secret_name = (
            f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        )
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode("UTF-8")

    @abstractmethod
    def read_metadata(self, log: bool = False):
        """
        Read the particular type of metadata from the targeted environment. We only need the project ID and the
        environment name to do so, which are encapsulated under class attributes.
        Args:
            log: Whether or not to print the metadata to stdout.
        Returns:
            A list of strings, where each string is the identifier of the particular piece of metadata.
        """

    def delete_metadata(self, metadata_type: str, metadata_identifier: str) -> None:
        """
        Given a type of metadata, delete a particular piece of metadata given an identifying ID. Since the same URI
        is used by the API for each type of metadata, we can implement this in a parent class.
        Args:
            metadata_type: The type of metadata as defined by the API.
            metadata_identifier: The identifying name of the piece of metadata to delete.

        Returns:
            None
        """
        logging.info(f"Deleting {metadata_identifier} from {metadata_type}")
        response = self._request(
            f"{self.webserver_url}/{metadata_type}/{metadata_identifier}",
            method="DELETE",
        )
        if response.status_code != 204:
            response.raise_for_status()

    @abstractmethod
    def apply_metadata(self, manifest_data: Dict[str, Dict]) -> None:
        """
        Given a manifest, apply the metadata to a particular environment.
        Args:
            manifest_data: Manifest data to apply to environment, in the form of a Dict.
        Returns:
            None
        """
