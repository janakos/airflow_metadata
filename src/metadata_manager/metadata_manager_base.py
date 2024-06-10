import argparse
import json
import logging
import sys
from typing import Optional, Tuple

from .connections import ConnectionManager
from .dags import DagManager
from .pools import PoolManager
from .roles import RoleManager
from .variables import VariableManager

ALLOWED_OPERATIONS = ["list", "apply"]
ALLOWED_METADATA_TYPES = ["pools", "connections", "dags", "roles", "variables"]


def load_metadata(filepath: str):
    with open(filepath, encoding="UTF-8") as file:
        metadata = json.load(file)
    return metadata


def parse_configuration(
    path: str, project_id: Optional[str] = None, environment_name: Optional[str] = None
) -> Tuple:
    """
    Given a path to a metadata manifest, extract the path, project_id, and data itself describing
    the metadata configuration. This will only get called if a path is supplied via the CLI.
    Project ID & environment name are specified in each manifest because each manifest is
    unique to an environment.
    Args:
        path: Path to the metadata manifest file to parse.
        project_id: Project ID supplied in the file.
        environment_name: Name of the target environment.
    Returns:
        Tuple of (data, project_id, environment_name, metadata_type)
    """
    data = None
    metadata_type = None
    if path:
        metadata = load_metadata(path)
        project_id = metadata["project_id"] if project_id is None else project_id
        environment_name = (
            metadata["environment_name"]
            if environment_name is None
            else environment_name
        )
        data = metadata.get("data", metadata)

        # Here, we specify the default value as `dags` since it's the only metadata type
        # which doesn't comply with our specification. TODO: Make the DAG manifest comply
        # with the data specification that pools/roles/variables follow.
        metadata_type = metadata.get("metadata_type", "dags")

        if environment_name is None:
            logging.error(
                "Must supply --environment-name for abstract metadata manifest."
            )
            sys.exit(1)
    return data, project_id, environment_name, metadata_type


def main(argv=None) -> int:
    """
    Main function of the script. Parses CLI arguments & return an integer to indicate success.
    Args:
        argv: Array of CLI commands

    Returns:
        0 if success
    """
    # Create the argparse parser
    parser = argparse.ArgumentParser(
        description="A tool used to update an Airflow environment's metadata"
    )
    parser.add_argument("-v", action="store_true", help="Verbose logging")
    subparsers = parser.add_subparsers(dest="command")

    # Create the list command
    list_command = subparsers.add_parser(
        "list", help="The type of metadata command to interact with."
    )
    list_command.add_argument("metadata_type", choices=ALLOWED_METADATA_TYPES)
    list_command.add_argument(
        "--environment-name", required=True, help="The name of the target environment."
    )
    list_command.add_argument(
        "--project-id", required=True, help="The project ID of the target environment."
    )
    list_command.add_argument(
        "--dag-manifest", help="If listing DAGs, provide path to the existing metadata."
    )

    # Create the apply command. For sensitive metadata types like connections we don't
    # support manifests, so we XOR on metadata_type|path.
    apply_command = subparsers.add_parser(
        "apply", help="The types of metadata command to interact with."
    )
    group = apply_command.add_mutually_exclusive_group(required=True)
    group.add_argument("--metadata_type", choices=ALLOWED_METADATA_TYPES, nargs="?")
    group.add_argument(
        "--path", help="The path to a metadata manifest to apply.", nargs="?"
    )
    apply_command.add_argument(
        "--environment-name", help="The name of the target environment."
    )
    apply_command.add_argument(
        "--project-id", help="The project ID of the target environment."
    )
    apply_command.add_argument(
        "--pause-all",
        action=argparse.BooleanOptionalAction,
        help="Pause all DAGs in target environment.",
    )

    # Parse the command-line arguments, set up logging
    args = parser.parse_args(args=argv)
    if args.command is None:
        parser.print_help()
        exit(3)
    logging.basicConfig(level=logging.DEBUG if getattr(args, "v") else logging.INFO)

    # Parse the configuration file, if one is passed. Otherwise, rely on CLI flags
    if getattr(args, "path", None) is not None:
        data, project_id, environment_name, metadata_type = parse_configuration(
            args.path, args.project_id, args.environment_name
        )
        setattr(args, "metadata_type", metadata_type)
    elif hasattr(args, "environment_name") and hasattr(args, "project_id"):
        data = None
        project_id = args.project_id
        environment_name = args.environment_name
    else:
        logging.error(
            "Must provide either --path or --environment-name and --project-id."
        )
        sys.exit(1)

    # Perform the appropriate command based on the arguments. No need to check for
    # unsupported metadata type because argparse would raise errors.
    try:
        # Add each kwarg from parsed args but update with overrides.
        kwargs = {attribute: getattr(args, attribute) for attribute in dir(args)}
        kwargs.update({"project_id": project_id, "environment_name": environment_name})

        manager = None
        if args.metadata_type == "variables":
            manager = VariableManager(**kwargs)
        elif args.metadata_type == "pools":
            manager = PoolManager(**kwargs)
        elif args.metadata_type == "connections":
            manager = ConnectionManager(**kwargs)
        elif args.metadata_type == "dags":
            manager = DagManager(**kwargs)
        elif args.metadata_type == "roles":
            manager = RoleManager(**kwargs)

        if args.command == "list":
            manager.read_metadata(log=True)
        elif args.command == "apply":
            manager.apply_metadata(data)
    except NotImplementedError:
        logging.error("Command unsupported for the specified metadata type.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
