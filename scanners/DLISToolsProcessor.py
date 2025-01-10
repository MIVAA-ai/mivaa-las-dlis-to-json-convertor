"""
This is an incomplete class it needs more refactoring
"""

import pandas as pd
from utils.dlis_utils import summary_dataframe

class DLISToolsProcessor:
    """
    Processes the tools in a DLIS logical file and handles extraction and transformation.
    """

    def __init__(self, logical_file_id, tools, nulls_list=None):
        """
        Initialize the DLISToolsProcessor.

        Args:
            logical_file_id (str): Unique identifier for the logical file.
            tools (list): List of tool objects in the logical file.
            nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
        """
        self._logical_file_id = logical_file_id
        self._tools = tools
        self._nulls_list = nulls_list or []

    def extract_tools(self):
        """
        Converts a DLIS tools DataFrame into the desired JSON-like format.

        Returns:
            dict: Tools information formatted as specified.
        """
        # Read tools into a DataFrame
        tools_df = self._extract_tools_df()
        print(tools_df)

        # # Initialize the structure for tools information
        # tools_info = {
        #     "attributes": ["name", "type", "description"],
        #     "objects": {}
        # }
        #
        # # Loop through the DataFrame rows
        # for _, row in tools_df.iterrows():
        #     # Extract tool information
        #     name = row["name"]
        #     tool_type = row["type"]
        #     description = row.get("description", None)  # Handle missing descriptions
        #
        #     # Trim spaces in the name, type, description
        #     name = name.strip() if isinstance(name, str) else name
        #     tool_type = tool_type.strip() if isinstance(tool_type, str) else tool_type
        #     description = description.strip() if isinstance(description, str) else description
        #
        #     # Add tool to the objects section
        #     tools_info["objects"][name] = [tool_type, description]
        #
        # return tools_info

    def _extract_tools_df(self):
        """
        Extracts and processes tools into a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing tools data.
        """
        tools_df = summary_dataframe(
            self._tools, description='description', trademark_name='trademark_name', generic_name='generic_name', status='status', parts='parts', channels='channels', parameters='parameters'
        )

        if tools_df.empty:
            print(f"No tools found for logical file: {self._logical_file_id}")
            return tools_df

        try:
            # Add logical file ID
            tools_df["logical-file-id"] = self._logical_file_id

            # Remove rows with null values
            tools_df = tools_df[~tools_df.isin(self._nulls_list).any(axis=1)]

            # Clean and deduplicate the DataFrame
            # tools_df = tools_df.drop_duplicates(subset=["name", "type"], ignore_index=True)
            tools_df = tools_df.drop_duplicates(ignore_index=True)

            return tools_df

        except Exception as e:
            print(f"Error processing tools for logical file {self._logical_file_id}: {e}")
            raise