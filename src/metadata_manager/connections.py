import json
import logging
from typing import List

from .api import ApiManager


class ConnectionManager(ApiManager):
    """
    This class manages connection metadata.
    """

    def read_metadata(self, log=False) -> List[str]:
        connection_names = []
        offset = 0
        while True:
            response = self._request(
                f"{self.webserver_url}/connections",
                method="GET",
                params={"offset": str(offset)},
            )
            connections_data = json.loads(response.text)["connections"]

            if not connections_data:
                break
            if response.status_code != 200:
                response.raise_for_status()
            else:
                connection_names += [
                    connection["connection_id"] for connection in connections_data
                ]
            offset += 100

        if log:
            logging.info(f"Connections in {self.environment_name}:")
            for connection in connection_names:
                logging.info(connection)
        return connection_names

    @staticmethod
    def __delete_kv_if_value_null(connection_data: dict) -> dict:
        """
        Remove all Key/Value pairs where Value == Null
        Airflow REST API experienced errors while parsing these because they are not actual fields
        Null values are return by default by Airflow CLI connections export command
        Args:
          connection_data dict: connection metadata
        """
        return {k: v for k, v in connection_data.items() if v is not None}

    def apply_metadata(self, manifest_data) -> None:
        # Generate a list of all connections in the target environment
        polled_connections = self.read_metadata()

        # Unlike other metadata, all connections contain sensitive data so we don't
        # use any local manifest data. Since we have unique connections per environment, we
        # namespace by environment name.
        manifest_data = json.loads(
            self.get_secret(f"{self.environment_name}-connections")
        )

        # Remove connections that exist in the environment but don't belong in the manifest
        # so that we can remove them to match the manifest state. Never remove the
        # all powerful airflow_db connection.
        connections_to_delete = (
            set(polled_connections) - set(manifest_data.keys()) - {"airflow_db"}
        )
        for connection in connections_to_delete:
            super().delete_metadata("connections", connection)

        # Now ensure that every connection in the manifest exists or is updated.
        for connection_name, connection_data in manifest_data.items():
            # Update the connection dict with the connection ID because the block doesn't
            # include it.
            logging.info(f"Updating {connection_name}")
            connection_data.update({"connection_id": connection_name})
            connection_data = self.__delete_kv_if_value_null(connection_data)
            response = self._request(
                f"{self.webserver_url}/connections/{connection_name}",
                method="PATCH",
                json=connection_data,
            )

            # If we get a 404, we need to create the connection instead of update it.
            if response.status_code == 404:
                logging.info(f"Creating connection {connection_name}")
                response = self._request(
                    f"{self.webserver_url}/connections",
                    method="POST",
                    json=connection_data,
                )
                if response.status_code != 200:
                    response.raise_for_status()
            elif response.status_code != 200:
                response.raise_for_status()
