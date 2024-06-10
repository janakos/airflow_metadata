import json
import logging
from typing import Dict, List

from .api import ApiManager


class VariableManager(ApiManager):
    def read_metadata(self, log: bool = False) -> List[str]:
        variable_names = []
        offset = 0
        while True:
            response = self._request(
                f"{self.webserver_url}/variables",
                method="GET",
                params={"offset": str(offset)},
            )
            variables_data = json.loads(response.text)["variables"]

            # Break from while loop once all variables are retrieved
            if not variables_data:
                break
            elif response.status_code != 200:
                response.raise_for_status()
            else:
                variable_names += [variable["key"] for variable in variables_data]
            offset += 100

        if log:
            logging.info(f"Variables in {self.environment_name}:")
            for name in variable_names:
                logging.info(name)
        return variable_names

    def apply_metadata(self, manifest_data: Dict[str, Dict]) -> None:
        """
        Since we only use this method to manage freshly deployed developer environments
        as of today, we don't trim old values like we do for other metadata updates.
        """

        # Now ensure that every variable in the manifest exists or is updated. The PATCH
        # method here will create the variable if it doesn't exist.
        for var_name, var_value in manifest_data.items():
            payload = {
                "key": var_name,
                "value": var_value,
            }
            logging.info(f"Updating variable {var_name}")
            response = self._request(
                f"{self.webserver_url}/variables/{var_name}",
                method="PATCH",
                json=payload,
            )

            if response.status_code != 200:
                response.raise_for_status()
