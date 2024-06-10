import json
import logging
from typing import Dict, List

from .api import ApiManager


class PoolManager(ApiManager):
    def read_metadata(self, log=False) -> List[str]:
        response = self._request(f"{self.webserver_url}/pools", method="GET")
        pools_data = json.loads(response.text)["pools"]

        if log:
            logging.info(f"Pools in {self.environment_name}:")
            for pool in pools_data:
                logging.info(f"{pool['name']} - {pool['slots']}")
        return [pool["name"] for pool in pools_data]

    def apply_metadata(self, manifest_data: Dict) -> None:
        # Generate a list of all pools in the target environment
        polled_pools = self.read_metadata()

        # Remove pools that exist in the environment but don't belong in the manifest
        # so that we can remove them to match the manifest state.
        pools_to_delete = set(polled_pools) - set(manifest_data.keys())
        for pool in pools_to_delete:
            super().delete_metadata("pools", pool)

        # Now ensure that every pool in the manifest exists or is updated.
        for pool_name, pool_configuration in manifest_data.items():
            logging.info(f"Updating {pool_name}")
            pool_configuration.update({"name": pool_name})
            response = self._request(
                f"{self.webserver_url}/pools/{pool_name}",
                method="PATCH",
                json=pool_configuration,
            )

            # If we get a 404, we need to create the pool instead of update it.
            # The PATCH method won't do it for us like with Variables.
            if response.status_code == 404:
                response = self._request(
                    f"{self.webserver_url}/pools",
                    method="POST",
                    json=pool_configuration,
                )
                if response.status_code != 200:
                    response.raise_for_status()
            elif response.status_code != 200:
                response.raise_for_status()
