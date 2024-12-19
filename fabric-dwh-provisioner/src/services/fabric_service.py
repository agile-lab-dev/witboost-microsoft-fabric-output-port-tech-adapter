from typing import Any, List
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
import struct
from itertools import chain, repeat
import requests


class FabricService:
    ## ESEGUIRE MODIFICA ----- get_sql_endpoint
    def __init__(self):
        """
        Initialize the FabricService with optional Azure credentials.
        """
        # If u wanto to try this in local use InteractiveBrowserCredential
        self.credential = InteractiveBrowserCredential()
        self.workspace_name = None
        self.dwh_name = None
        self.connection = None
        self.sql_endpoint = None


    def validate_workspace_and_dwh(self):
        """Check that workspace and DWH names are set."""
        if not self.workspace_name:
            raise ValueError("Workspace name is not set. Use 'set_workspace' to provide it.")
        if not self.dwh_name:
            raise ValueError("DWH name is not set. Use 'set_dwh' to provide it.")

    def get_headers(self, scope: str) -> dict:
        token = self.credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}"}

    def find_workspace(self) -> dict:
        self.validate_workspace_and_dwh()
        url = "https://api.powerbi.com/v1.0/myorg/groups"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        workspaces = response.json()["value"]
        workspace = next((w for w in workspaces if w["name"] == self.workspace_name), None)
        if not workspace:
            raise ValueError(f"Workspace '{self.workspace_name}' not found.")
        return workspace

    def find_dwh(self, workspace_id: str) -> dict:
        self.validate_workspace_and_dwh()
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        warehouses = response.json()["value"]
        dwh = next((w for w in warehouses if w["displayName"] == self.dwh_name), None)
        if not dwh:
            raise ValueError(f"DWH '{self.dwh_name}' not found in workspace '{self.workspace_name}'.")
        return dwh

    def get_sql_endpoint(self,workspace:str,dwh:str) -> str:
        self.workspace_name = workspace
        self.dwh_name = dwh
        self.validate_workspace_and_dwh()
        workspace = self.find_workspace()
        dwh = self.find_dwh(workspace["id"])

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace['id']}/warehouses/{dwh['id']}"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        self.sql_endpoint = response.json()["properties"]["connectionString"]
        print(f"SQL Endpoint found: {self.sql_endpoint}")
        return self.sql_endpoint

    def connect(self):
        if not self.sql_endpoint:
            self.get_sql_endpoint()

        self.connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.sql_endpoint};"
            f"Database={self.dwh_name};"
            f"TrustServerCertificate=Yes;"
        )

        token_object = self.credential.get_token("https://database.windows.net/.default")
        token_as_bytes = bytes(token_object.token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        attrs_before = {1256: token_bytes}

        self.connection = pyodbc.connect(self.connection_string, attrs_before=attrs_before)
        print("Connection to DWH successfully established.")


    def execute_definition_query(self, query: str, params: List[Any] = None):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or [])
            self.connection.commit()
            print("Operation completed successfully.")
        except (pyodbc.Error, pyodbc.ProgrammingError) as e:
            raise RuntimeError(f"Error executing query: {e}")
        finally:
            cursor.close()



    def create_table(self, table_name: str, schema: str) -> bool:
        """
        Create a table in the DWH.
        :param table_name: Name of the table to create.
        :param schema: The SQL schema for the table.
        """
        
        query = f"CREATE TABLE {table_name} ({schema})"
        print(f"Creating table '{table_name}' with schema: {schema}")
        self.execute_definition_query(query)
        return True

    def drop_table(self, table_name: str) -> bool:
        """
        Delete a table from the DWH, if it exists.
        :param table_name: Name of the table to delete.
        """
        query = f"DROP TABLE IF EXISTS {table_name}"
        print(f"Drop table: '{table_name}' if exist")
        self.execute_definition_query(query)
        return True

    def apply_acl_to_table(self, acl_entries, table_name, provisioning=False)-> bool:
            """
            Connect to the DWH and apply ACL entries to a specific table.
            :param acl_entries: List of ACL entries (e.g., groups or users).
            :param table_name: The table in the DWH for which to assign permissions.
            """
            if not self.connection:
                self.connect()
            try:
                print("Connecting to DWH...")
                cursor = self.connection.cursor()
                for acl_entry in acl_entries:
                    if provisioning == True:
                        grant_query = f"""
                            GRANT ALL PRIVILEGES ON {table_name} TO [{acl_entry}];
                        """
                        print(f"Executing query: {grant_query}")
                        cursor.execute(grant_query)
                    else:
                        # Assign permission trough T-SQL
                        grant_query = f"""
                            GRANT SELECT ON {table_name} TO [{acl_entry}];
                        """
                        print(f"Executing query: {grant_query}")
                        cursor.execute(grant_query)

                cursor.commit()
                print(f"Successfully updated ACL for table '{table_name}'.")
                return True
            except Exception as e:
                print(f"Error applying ACL to table: {e}")
                return False

    def close(self):
        """
        Close the connection to DWH
        """
        if self.connection:
            self.connection.close()
            print("Connection to DWH closed")


