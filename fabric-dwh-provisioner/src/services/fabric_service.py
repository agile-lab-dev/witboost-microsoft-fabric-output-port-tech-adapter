import struct
from itertools import chain, repeat
from typing import Any, List

import pyodbc  # type: ignore
import requests
from azure.identity import DefaultAzureCredential

from src.utility.logger import get_logger


class FabricService:
    def __init__(self):
        """
        Initialize the FabricService with optional Azure credentials.
        """
        self.credential = DefaultAzureCredential()
        self.workspace_name = None
        self.dwh_name = None
        self.connection = None
        self.sql_endpoint = None
        self.lakehouse_name = None
        self.logger = get_logger(__name__)

    def get_headers(self, scope: str) -> dict:
        token = self.credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}"}

    def find_workspace(self) -> dict:
        url = "https://api.powerbi.com/v1.0/myorg/groups"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        workspaces = response.json()["value"]
        workspace = next(
            (w for w in workspaces if w["name"] == self.workspace_name), None
        )
        if not workspace:
            raise ValueError(f"Workspace '{self.workspace_name}' not found.")
        return workspace

    def find_dwh(self, workspace_id: str) -> dict:
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
        )
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        warehouses = response.json()["value"]
        dwh = next((w for w in warehouses if w["displayName"] == self.dwh_name), None)
        if not dwh:
            raise ValueError(
                f"DWH '{self.dwh_name}' not found in workspace '{self.workspace_name}'."
            )
        return dwh

    def find_lakehouse(self, workspace_id: str) -> dict:
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        )
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        warehouses = response.json()["value"]
        lakehouse = next(
            (w for w in warehouses if w["displayName"] == self.lakehouse_name), None
        )
        if not lakehouse:
            raise ValueError(
                f"DWH '{self.lakehouse_name}' not found in workspace.'{self.workspace_name}'"
            )
        return lakehouse

    def get_sql_endpoint(
        self,
        workspace_name: str,
        dwh_name: str | None = None,
        lakehouse_name: str | None = None,
    ) -> str:
        self.workspace_name = workspace_name
        self.dwh_name = dwh_name
        self.lakehouse_name = lakehouse_name
        if dwh_name is not None and lakehouse_name is None:
            workspace = self.find_workspace()
            dwh = self.find_dwh(workspace["id"])
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace['id']}/warehouses/{dwh['id']}"
            headers = self.get_headers(
                "https://analysis.windows.net/powerbi/api/.default"
            )
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            self.sql_endpoint = response.json()["properties"]["connectionString"]
            self.logger.info(f"SQL Endpoint found: {self.sql_endpoint}")
            return self.sql_endpoint
        elif dwh_name is None and lakehouse_name is not None:
            # This code could be useful to get sqlEndpoint for lakehouse, with this could be possible to do ql
            workspace = self.find_workspace()
            lake = self.find_lakehouse(workspace["id"])
            lakehouse = lake["properties"]["sqlEndpointProperties"]["connectionString"]
            return lakehouse
        raise ValueError("Unable to determine the SQL Endpoint")

    def connect(self):
        if not self.sql_endpoint:
            self.get_sql_endpoint()

        self.connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.sql_endpoint};"
            f"Database={self.dwh_name};"
            f"TrustServerCertificate=Yes;"
        )

        token_object = self.credential.get_token(
            "https://database.windows.net/.default"
        )
        token_as_bytes = bytes(token_object.token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        attrs_before = {1256: token_bytes}

        self.connection = pyodbc.connect(
            self.connection_string, attrs_before=attrs_before
        )
        self.logger.info("Connection to DWH successfully established.")

    def execute_definition_query(self, query: str, params: List[Any] | None = None):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or [])
            self.connection.commit()
            self.logger.info("Operation completed successfully.")
        except (pyodbc.Error, pyodbc.ProgrammingError):
            self.logger.exception("Exception in execute_definition_query")
            raise
        finally:
            cursor.close()

    def create_table(self, table_name: str, schema: str) -> bool:
        """
        Create a table in the DWH.
        :param table_name: Name of the table to create.
        :param schema: The SQL schema for the table.
        """
        query = f"CREATE TABLE {table_name} ({schema})"
        self.logger.info(f"Creating table '{table_name}' with schema: {schema}")
        self.execute_definition_query(query)
        return True

    def drop_table(self, table_name: str) -> bool:
        """
        Delete a table from the DWH, if it exists.
        :param table_name: Name of the table to delete.
        """
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.logger.info(f"Drop table: '{table_name}' if exist")
        self.execute_definition_query(query)
        return True

    def apply_acl_to_dwh_table(
        self, acl_entries, table_name, provisioning=False
    ) -> bool:
        """
        Connect to the DWH and apply ACL entries to a specific table.
        :param acl_entries: List of ACL entries (e.g., groups or users).
        :param table_name: The table in the DWH for which to assign permissions.
        """
        if not self.connection:
            self.connect()
        try:
            self.logger.info("Connecting to DWH...")
            cursor = self.connection.cursor()
            for acl_entry in acl_entries:
                if provisioning:
                    grant_query = f"""
                            GRANT ALL PRIVILEGES ON {table_name} TO [{acl_entry}];
                        """
                    self.logger.info(f"Executing query: {grant_query}")
                    cursor.execute(grant_query)
                else:
                    # Assign permission trough T-SQL
                    grant_query = f"""
                            GRANT SELECT ON {table_name} TO [{acl_entry}];
                        """
                    self.logger.info(f"Executing query: {grant_query}")
                    cursor.execute(grant_query)

            cursor.commit()
            self.logger.info(f"Successfully updated ACL for table '{table_name}'.")
            return True
        except Exception:
            self.logger.exception("Error applying ACL to table")
            return False

    def load_table(
        self,
        workspace_id: str,
        lakehouse_id: str,
        table_name: str,
        relative_path: str,
        file_format: str,
    ) -> bool:
        """
        Create a table in the Lakehouse from file.
        :param workspace_id: Name of the workspace.
        :param lakehouse_id: Name of the lakehouse.
        :param table_name: Name of a new Table
        :relative_path: File path for create Table
        """
        self.workspace_name = workspace_id
        workspace_id_f = self.find_workspace()
        self.lakehouse_name = lakehouse_id
        lakehouse_id_f = self.find_lakehouse(workspace_id_f["id"])
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id_f['id']}/lakehouses/{lakehouse_id_f['id']}/tables/{table_name}/load"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        # The entire payload can be customized
        payload = {
            "relativePath": relative_path,
            "pathType": "File",
            "mode": "Overwrite",
            "recursive": False,
            "formatOptions": {"format": file_format, "header": True, "delimiter": ","},
        }

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 202:
            self.logger.info(
                f"Table '{table_name}' loaded successfully from '{relative_path}'."
            )
            return True
        else:
            self.logger.error(
                f"Failed to load table '{table_name}'. Response: {response.status_code}, {response.text}"
            )
            return False

    def close(self):
        """
        Close the connection to DWH
        """
        if self.connection:
            self.connection.close()
            self.logger.info("Connection to DWH closed")
