import requests
from azure.identity import InteractiveBrowserCredential
from azure.identity import DefaultAzureCredential
import pyodbc

class AzureFabricApiService:

    def __init__(self):
        """
        Autentication Initialize with Azure ADD
        """
        # If u wanto to try this in local use InteractiveBrowserCredential
        self.credential = InteractiveBrowserCredential()
        self.token = self.credential.get_token("https://graph.microsoft.com/.default").token
        self.headers = {"Authorization": f"Bearer {self.token}"}


    def get_group_id(self, group_name, headers):
        """
        Retrieve the ID of a group from its display name using Microsoft Graph API.
        :param group_name: The display name of the group.
        :param headers: Headers containing the authorization token.
        :return: The group ID.
        """
        try:
            graph_endpoint = f"https://graph.microsoft.com/v1.0/groups?$filter=displayName eq '{group_name}'"
            response = requests.get(graph_endpoint, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch group ID for '{group_name}': {response.text}")

            groups = response.json().get("value", [])
            if not groups:
                raise ValueError(f"Group '{group_name}' not found.")

            return groups[0]["id"]
        except Exception as e:
            print(f"Error fetching group ID: {e}")
            raise

    def get_user_id(self, user_name, headers):
        """
        Retrieve the ID of a user from its user principal name (email) using Microsoft Graph API.
        :param user_name: The user principal name (email) of the user.
        :param headers: Headers containing the authorization token.
        :return: The user ID.
        """
        try:
            graph_endpoint = f"https://graph.microsoft.com/v1.0/users?$filter=userPrincipalName eq '{user_name}'"
            response = requests.get(graph_endpoint, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch user ID for '{user_name}': {response.text}")

            users = response.json().get("value", [])
            if not users:
                raise ValueError(f"User '{user_name}' not found.")

            return users[0]["id"]
        except Exception as e:
            print(f"Error fetching user ID: {e}")
            raise

    def update_acl(self, entities):
        """
        Update ACL in the DWH to assign read-only permissions to a specific table.
        :param entities: List of Azure AD group names/IDs or user principal names (emails).
        :return: A list of ACL entries.
        """
        try:
            # Define known prefixes for groups and users
            GROUP_PREFIX = "group:"
            USER_PREFIX = "user:"

            # Authentication via Azure CLI
            token = self.credential.get_token("https://graph.microsoft.com/.default").token

            headers = {"Authorization": f"Bearer {token}"}

            acl_entries = []

            print(f"Processing {len(entities)} entities...")
            for entity in entities:
                # Determines whether the entity is a group or a user based on the prefix
                if entity.startswith(USER_PREFIX):
                    # Convert the user's name to ID if necessary
                    parts = entity.rsplit("_",1)
                    entity = "@".join(parts)
                    user_name = entity[len(USER_PREFIX):] 
                    user_id = self.get_user_id(user_name, headers)

                    user_endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}"
                    user_response = requests.get(user_endpoint, headers=headers)
                    if user_response.status_code != 200:
                        raise Exception(f"Failed to validate user {user_id}: {user_response.text}")
                    user_data = user_response.json()
                    print(f"Validated user: {user_data.get('displayName', 'Unknown')} ({user_id})")
                    mail = user_data.get('mail')
                    acl_entries.append(f"{mail}")

                elif entity.startswith(GROUP_PREFIX):
                    
                    group_name = entity[len(GROUP_PREFIX):] 
                    group_id = self.get_group_id(group_name, headers)

                    group_endpoint = f"https://graph.microsoft.com/v1.0/groups/{group_id}"
                    group_response = requests.get(group_endpoint, headers=headers)
                    if group_response.status_code != 200:
                        raise Exception(f"Failed to validate group {group_id}: {group_response.text}")
                    group_data = group_response.json()
                    mailNickName = group_data.get('mailNickname')
                    print(f"Validated group: {group_data.get('displayName', 'Unknown')} ({group_id})")
                    acl_entries.append(f"{mailNickName}")

                else:
                    raise ValueError(f"Unknown entity type for '{entity}'. Must start with '{USER_PREFIX}' or '{GROUP_PREFIX}'.")

            if not acl_entries:
                raise ValueError("No valid groups or users provided.")
            else:
                return acl_entries

        except Exception as e:
            print(f"Error updating ACL: {e}")
            raise

