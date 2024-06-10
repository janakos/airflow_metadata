import json
import logging
import re
from typing import Dict

from .api import ApiManager


class DagManager(ApiManager):
    # [callen]: Since the imports in the file are so lean, I don't want to add a potentiallly huge import overhead.
    # TODO: Cleanup imports in iguana_constants.py and use DagTag.LATEST_CRITICAL_PATH
    TAG_LATEST_CRITICAL_PATH = "latest_pipeline:critical_path"
    # TODO: Cleanup imports in iguana_constants.py and use DagTag.BATCH_CRITICAL_PATH
    TAG_BATCH_CRITICAL_PATH = "batch_pipeline:critical_path"
    TAGS_CRITICAL_PATH = [TAG_LATEST_CRITICAL_PATH, TAG_BATCH_CRITICAL_PATH]

    CUSTOM_METADATA_FIELDS = ["max_task_duration_minutes", "max_dag_duration_minutes"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def local_metadata_extract_custom_fields(self) -> dict:
        if not self.dag_manifest:
            logging.error("Must provide --dag-manifest flag if listing DAG states.")
            exit(1)
        with open(self.dag_manifest, encoding="UTF-8") as file:
            dag_metadata = json.load(file)
        data = {}
        for k, v in dag_metadata.items():
            data[k] = {}
            for field, val in v.items():
                if field in self.CUSTOM_METADATA_FIELDS:
                    data[k][field] = val
        return {k: v for k, v in data.items() if v}

    @staticmethod
    def update_remote_metadata(metadata: dict, custom_fields: dict) -> dict:
        for k, v in metadata.items():
            if k in custom_fields:
                for field, val in custom_fields[k].items():
                    metadata[k][field] = val
        return metadata

    def read_metadata(
        self, *, log: bool = False, fail_on_import_error: bool = True
    ) -> Dict:
        results = {}
        offset = 0
        response = self._request(
            f"{self.webserver_url}/importErrors",
            method="GET",
        )
        import_errors = json.loads(response.text)["total_entries"]
        if import_errors != 0 and fail_on_import_error:
            print(
                f"{import_errors} import  errors detected on this environment, aborting."
            )
            exit(1)
        while True:
            response = self._request(
                f"{self.webserver_url}/dags",
                method="GET",
                params={"offset": str(offset)},
            )
            dags = json.loads(response.text)["dags"]
            if not dags:
                break
            elif response.status_code != 200:
                response.raise_for_status()
            else:
                for dag in dags:
                    owners = [
                        i.strip() for i in dag["owners"] if i.strip() != "airflow"
                    ]
                    tag_values = [item["name"] for item in dag["tags"]]
                    target_environments = ",".join(
                        [i for i in tag_values if re.search(r"env_.*", i)]
                    )
                    if not target_environments:
                        # If a DAG has no designated target environment, it's best that we assume
                        # it runs everywhere
                        target_environments = "env_all"
                    is_critical_path = bool(
                        [
                            i["name"]
                            for i in dag["tags"]
                            if i["name"] in self.TAGS_CRITICAL_PATH
                        ]
                    )
                    results[dag["dag_id"]] = {
                        "owners": owners if owners else ["data-infra"],
                        "is_critical_path": is_critical_path,
                        "is_paused": dag["is_paused"],
                        "target_environments": target_environments,
                    }
            offset += 100

        custom_field_metadata = self.local_metadata_extract_custom_fields()
        results = self.update_remote_metadata(results, custom_field_metadata)

        if log:
            print(json.dumps(results, sort_keys=True, indent=4, separators=(",", ": ")))
        return results

    def apply_metadata(self, manifest_data: Dict[str, Dict]) -> None:
        """
        Given a DAG manifest, apply paused states to an environment.
        Args:
            manifest_data: a dict mapping a DAG ID to it's `paused` state, among other
            metadata that we don't interact with.

        Returns:
            None
        """
        for dag_id, dag_data in manifest_data.items():
            dag_state = True if self.pause_all else dag_data["is_paused"]

            logging.info(f"Setting {dag_id} paused: {dag_state}")
            response = self._request(
                f"{self.webserver_url}/dags/{dag_id}?update_mask=is_paused",
                method="PATCH",
                json={"is_paused": dag_state},
            )
            if response.status_code != 200:
                response.raise_for_status()
