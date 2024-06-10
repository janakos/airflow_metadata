import logging
from typing import Dict

from .api import ApiManager


class RoleManager(ApiManager):
    def read_metadata(self, log: bool = False):
        raise NotImplementedError

    def apply_metadata(self, manifest_data: Dict[str, Dict]) -> None:
        """
        With version of apply_metadata, we don't purge old metadata that doesn't conform
        to our manifest. We just use it to update the `User` role for now.
        Args:
            manifest_data: The manifest to apply.
        Returns:
            None
        """
        for role_name, role_value in manifest_data.items():
            payload = {
                "name": role_name,
                "actions": role_value["actions"],
            }
            logging.info(f"Updating role {role_name}")
            response = self._request(
                f"{self.webserver_url}/roles/{role_name}",
                method="PATCH",
                json=payload,
            )

            if response.status_code != 200:
                response.raise_for_status()
