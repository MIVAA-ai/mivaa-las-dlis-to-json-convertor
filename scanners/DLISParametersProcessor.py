import pandas as pd
import numpy as np
from utils.dlis_utils import summary_dataframe

class DLISParametersProcessor:
    """
    Processes the parameters in a DLIS logical file and handles extraction and transformation.
    """
    def __init__(self, logical_file_id, parameters, nulls_list=None):
        """
        Initialize the DLISParametersProcessor.

        Args:
            logical_file_id (str): Unique identifier for the logical file.
            parameters (list): List of parameter objects in the logical file.
            nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
        """
        self._logical_file_id = logical_file_id
        self._parameters = parameters
        self._nulls_list = nulls_list or []

    def extract_parameters(self):
        """
        Converts a DLIS parameters DataFrame into the desired JSON-like format.

        Args:
            param_df (pd.DataFrame): DataFrame containing DLIS parameters with columns
                                     ['name', 'value', 'unit', 'description'].

        Returns:
            dict: Parameter information formatted as specified.
        """
        #reading the parameters in dataframe
        param_df = self._extract_parameters_df()

        # Initialize the structure for parameter information
        parameter_info = {
            "attributes": ["value", "unit", "description"],
            "objects": {}
        }

        # Loop through the DataFrame rows
        for _, row in param_df.iterrows():
            # Extract parameter information
            name = row["name"]
            value = row["value"]
            unit = row["unit"]
            description = row.get("description", None)  # Handle missing descriptions

            # Trim spaces in the name, unit, description
            name = name.strip() if isinstance(name, str) else name
            unit = unit.strip() if isinstance(unit, str) else unit
            description = description.strip() if isinstance(description, str) else description

            # Format value as string and trim spaces
            if isinstance(value, list):
                value = ", ".join(map(str.strip, value))  # Trim spaces in each element and join
            elif isinstance(value, (np.ndarray, tuple)):
                value = ", ".join(map(str.strip, value))  # Trim spaces in each element and join
            else:
                value = str(value).strip() if value is not None else None

            # Add parameter to the objects section
            parameter_info["objects"][name] = [value, unit, description]

        return parameter_info

    def _extract_parameters_df(self):
        """
        Extracts and processes parameters into a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing parameter data.
        """
        param_df = summary_dataframe(
            self._parameters, name="name", values="value", long_name="description"
        )

        if param_df.empty:
            print(f"No parameters found for logical file: {self._logical_file_id}")
            return param_df

        try:
            # Add units to the DataFrame
            param_df["unit"] = self._extract_units(param_df)

            # Add logical file ID
            param_df["logical-file-id"] = self._logical_file_id

            # Transform parameter values
            param_df["value"] = param_df.apply(self._transform_values, axis=1)

            # Remove rows with null values
            param_df = param_df[~param_df.isin(self._nulls_list).any(axis=1)]

            # Convert 'value' to a hashable type
            param_df["value"] = param_df["value"].apply(
                lambda v:
                v[0] if isinstance(v, (list, np.ndarray)) and len(v) == 1 else
                "; ".join(v) if isinstance(v, (list, np.ndarray)) else
                v
            )

            # Clean and deduplicate the DataFrame
            param_df = param_df.drop_duplicates(ignore_index=True)
            # param_df = param_df.drop_duplicates(subset=["name", "value"], ignore_index=True)

            return param_df

        except Exception as e:
            print(f"Error processing parameters for logical file {self._logical_file_id}: {e}")
            raise

    def _extract_units(self, param_df):
        """
        Extracts units from the parameters and aligns them with the DataFrame index.

        Args:
            param_df (pd.DataFrame): DataFrame containing parameter data.

        Returns:
            list: A list of units corresponding to the parameters.
        """
        units_column = []
        for i, param in enumerate(self._parameters):
            if i not in param_df.index:
                continue
            try:
                units_column.append(param.attic["VALUES"].units)
            except KeyError:
                units_column.append(None)
        return units_column

    @staticmethod
    def _transform_values(row):
        """
        Transforms the 'value' column in the DataFrame.

        Args:
            row (pd.Series): A row from the DataFrame.

        Returns:
            Any: Transformed value for the row.
        """
        if isinstance(row["value"], list):
            if len(row["value"]) == 1:
                return row["value"][0]
            return row["value"]
        return row["value"]