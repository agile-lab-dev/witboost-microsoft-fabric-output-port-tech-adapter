from typing import Any, List
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
import struct
from itertools import chain, repeat
import requests


class FabricService:
    
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
        self.lakehouse_name = None

    def get_headers(self, scope: str) -> dict:
        token = self.credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}"}
    

    def find_workspace(self) -> dict:
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
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        warehouses = response.json()["value"]
        dwh = next((w for w in warehouses if w["displayName"] == self.dwh_name), None)
        if not dwh:
            raise ValueError(f"DWH '{self.dwh_name}' not found in workspace '{self.workspace_name}'.")
        return dwh
    
    def find_lakehouse(self, workspace_id: str) -> dict:
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        warehouses = response.json()["value"]
        lakehouse = next((w for w in warehouses if w["displayName"] == self.lakehouse_name), None)
        if not lakehouse:
            raise ValueError(f"DWH '{self.lakehouse_name}' not found in workspace.'{self.workspace_name}'")
        return lakehouse

    def get_sql_endpoint(self,workspace:str, dwh:str | None = None, lake_house: str | None = None) -> str:
        self.workspace_name = workspace
        self.dwh_name = dwh
        self.lakehouse_name = lake_house
        if dwh is not None and lake_house is None:
            workspace = self.find_workspace()
            dwh = self.find_dwh(workspace["id"])
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace['id']}/warehouses/{dwh['id']}"
            headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            self.sql_endpoint = response.json()["properties"]["connectionString"]
            print(f"SQL Endpoint found: {self.sql_endpoint}")
            return self.sql_endpoint
        elif dwh is None and lake_house is not None:
            # This code could be useful to get sqlEndpoint for lakehouse, with this could be possible to do ql 
            workspace = self.find_workspace()
            lake = self.find_lakehouse(workspace["id"])
            lakehouse = lake['properties']['sqlEndpointProperties']['connectionString']
            return lakehouse

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

    def apply_acl_to_dwh_table(self, acl_entries, table_name, provisioning=False)-> bool:
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
            
    def apply_acl_to_lakehouse_table(self,workspace,lakehouse_id, acl_entries, table_name, provisioning=False)-> bool:
            """
            Connect to the DWH and apply ACL entries to a specific table.
            :param acl_entries: List of ACL entries (e.g., groups or users).
            :param table_name: The table in the DWH for which to assign permissions.
            """
            self.workspace_name = workspace
            workspace_id_f = self.find_workspace()
            self.lakehouse_name = lakehouse_id
            lakehouse_id_f = self.find_lakehouse(workspace_id_f['id'])
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id_f['id']}/items/{lakehouse_id_f['id']}/dataAccessRoles"
            headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
            try:
                # Step 1: Ottieni i ruoli esistenti
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    raise Exception(f"Errore nella GET: {response.status_code} - {response.text}")
                
                print(f"I ruoli sono questi! {response.json()}")
                role_name = "Admin"
                
                roles = response.json().get("value", [])
                role = next((r for r in roles if r['name'] == role_name), None)

                # Step 2: Prepara i membri (ottieni gli ID utente da email)
                acl_entries_l = [acl_entries]
                user_source_paths = []
                for email in acl_entries_l:
                    user_source_paths.append(f"{workspace_id_f['id']}/{email}")
                 
               
                # Step 3: Aggiorna i membri
                if role:
                    existing_members = role.get("members", {}).get("fabricItemMembers", [])
                    combined_members = {m['sourcePath']: m for m in existing_members + [{"sourcePath": source_path, "itemAccess": ["ReadAll"]} for source_path in user_source_paths]}
                else:
                    combined_members = {m['sourcePath']: m for m in [{"sourcePath": source_path, "itemAccess": ["ReadAll"]} for source_path in user_source_paths]}


                
                
                # Step 4: Prepara il payload per l'aggiornamento
                payload = { 
                    "value": [
                        {
                            "name": role_name,
                            "decisionRules": [
                                {
                                    "effect": "Permit",
                                    "permission": [
                                        {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                                        {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
                                    ]
                                }
                            ], 
                            "members": {"microsoftEntraMembers":[
    {
        "tenantId": "eee7e750-299f-468f-a6c3-9f28923f6133",
        "objectId": "23a64484-94c9-4f37-8fcb-288e4daeaadf"
    } ],"fabricItemMembers":[{"itemAccess": [
              "ReadAll"
            ],
            "sourcePath": "253ec260-d51a-4d3c-a654-155b665dc0c1/e6f170f6-26a4-4471-a9d8-f938c1631447"}] }
                        }
                    ]
                }

                # Step 5: Esegui la PUT per aggiornare il ruolo
                response = requests.put(url, headers=headers, json=payload)
                if response.status_code == 200:
                    print("Ruolo aggiornato con successo.")
                else:
                    raise Exception(f"Errore nella PUT: {response.status_code} - {response.text}")

            except Exception as e:
                print(f"Errore: {e}")


            
    def load_table(self, workspace_id: str, lakehouse_id: str, table_name: str, relative_path: str, file_format: str) -> bool:
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
        lakehouse_id_f = self.find_lakehouse(workspace_id_f['id'])
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id_f['id']}/lakehouses/{lakehouse_id_f['id']}/tables/{table_name}/load"
        headers = self.get_headers("https://analysis.windows.net/powerbi/api/.default")
        # The entire payload can be customized
        payload = {
            "relativePath": relative_path,
            "pathType": "File",
            "mode": "Overwrite",
            "recursive": False,
            "formatOptions": {
                "format": file_format,
                "header": True,
                "delimiter": ","
            }
        }

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 202:
            print(f"Table '{table_name}' loaded successfully from '{relative_path}'.")
            return True
        else:
            print(f"Failed to load table '{table_name}'. Response: {response.status_code}, {response.text}")
            return False


    def close(self):
        """
        Close the connection to DWH
        """
        if self.connection:
            self.connection.close()
            print("Connection to DWH closed")


