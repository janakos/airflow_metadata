# Project Name

## Overview

This project contains a collection of Python scripts for managing various aspects of a data platform. The scripts handle API interactions, database connections, DAG management, metadata management, pool management, role management, and variable management.

## Files

1. **api.py**
   - Handles API interactions and requests. It includes functions for sending requests to the API, processing responses, and handling errors. The script also manages authentication and authorization for secure API access.

2. **connections.py**
   - Manages database connections.

3. **dags.py**
   - Responsible for managing Directed Acyclic Graphs (DAGs).

4. **metadata_manager_base.py**
   - Base class for metadata management.

5. **pools.py**
   - Manages resource pools.

6. **roles.py**
   - Manages user roles and permissions.

7. **variables.py**
   - Handles environment variables and configurations.

## Installation

1. Clone the repository:
   ```sh
   git clone airflow_metadata
   pip install .
