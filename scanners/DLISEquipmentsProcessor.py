import pandas as pd
from pandas.core.dtypes.astype import astype_array

from utils.dlis_utils import summary_dataframe

class DLISEquipmentsProcessor:
    """
    Processes the equipment data in a DLIS logical file and handles extraction and transformation.
    """

    def __init__(self, logical_file_id, equipments, nulls_list=None):
        """
        Initialize the DLISEquipmentsProcessor.

        Args:
            logical_file_id (str): Unique identifier for the logical file.
            equipments (list): List of equipment objects in the logical file.
            nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
        """
        self._logical_file_id = logical_file_id
        self._equipments = equipments
        self._nulls_list = nulls_list or []

    def extract_equipments(self):
        """
        Converts a DLIS equipment DataFrame into the desired JSON-like format.

        Returns:
            dict: Equipment information formatted as specified.
        """
        # Read equipment data into a DataFrame
        equipments_df = self._extract_equipments_df()

        # print(equipments_df)

        # equipments_df.to_csv(rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed\param.csv')
        # Initialize the structure for equipment information
        equipments_info = {
            "attributes": [
                "trademark_name", "status", "generic_type", "serial_number", "location",
                "height", "length", "diameter_min", "diameter_max", "volume", "weight",
                "hole_size", "pressure", "temperature", "vertical_depth", "radial_drift", "angular_drift"
            ],
            "objects": []
        }

        # Loop through the DataFrame rows
        for _, row in equipments_df.iterrows():
            # Extract relevant fields
            equipment_details = {
                "trademark_name": row.get("trademark_name", "").strip() if isinstance(row.get("trademark_name"), str) else row.get("trademark_name"),
                "status": row.get("status", "").strip() if isinstance(row.get("status"), str) else row.get("status"),
                "generic_type": row.get("generic_type", "").strip() if isinstance(row.get("generic_type"), str) else row.get("generic_type"),
                "serial_number": row.get("serial_number", "").strip() if isinstance(row.get("serial_number"), str) else row.get("serial_number"),
                "location": row.get("location", "").strip() if isinstance(row.get("location"), str) else row.get("location"),
                "height": row.get("height"),
                "length": row.get("length"),
                "diameter_min": row.get("diameter_min"),
                "diameter_max": row.get("diameter_max"),
                "volume": row.get("volume"),
                "weight": row.get("weight"),
                "hole_size": row.get("hole_size"),
                "pressure": row.get("pressure"),
                "temperature": row.get("temperature"),
                "vertical_depth": row.get("vertical_depth"),
                "radial_drift": row.get("radial_drift"),
                "angular_drift": row.get("angular_drift")
            }

            # Add to objects list
            equipments_info["objects"].append(equipment_details)

        return equipments_info

    def _extract_equipments_df(self):
        """
        Extracts and processes equipment data into a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing equipment data.
        """
        # Pass the specified parameters dynamically
        equipments_df = summary_dataframe(
            self._equipments,
            name="name",
            trademark_name="trademark_name",
            status="status",
            generic_type="generic_type",
            serial_number="serial_number",
            location="location",
            height="height",
            length="length",
            diameter_min="diameter_min",
            diameter_max="diameter_max",
            volume="volume",
            weight="weight",
            hole_size="hole_size",
            pressure="pressure",
            temperature="temperature",
            vertical_depth="vertical_depth",
            radial_drift="radial_drift",
            angular_drift="angular_drift"
        )

        if equipments_df.empty:
            print(f"No equipment found for logical file: {self._logical_file_id}")
            return equipments_df

        try:
            # Extract units and append them to the values
            units = self._extract_units()
            for col, unit in units.items():
                if col in equipments_df.columns and unit:
                    equipments_df[col] = equipments_df[col].apply(
                        lambda v: f"{v} {unit}" if pd.notnull(v) else v
                    )

            # Add logical file ID
            equipments_df["logical-file-id"] = self._logical_file_id

            # Remove rows with null values
            equipments_df = equipments_df[~equipments_df.isin(self._nulls_list).any(axis=1)]

            # Clean and deduplicate the DataFrame
            equipments_df = equipments_df.drop_duplicates(ignore_index=True)

            return equipments_df

        except Exception as e:
            print(f"Error processing equipment for logical file {self._logical_file_id}: {e}")
            raise

    def _extract_units(self):
        """
        Extracts units for each equipment attribute.

        Returns:
            dict: A dictionary mapping attribute names to their units.
        """

        # units_column = []
        # for i, param in enumerate(self._parameters):
        #     if i not in param_df.index:
        #         continue
        #     try:
        #         units_column.append(param.attic["VALUES"].units)
        #     except KeyError:
        #         units_column.append(None)
        # return units_column

        units = {}
        for equip in self._equipments:
            for attr in [
                "length", "diameter_min", "diameter_max", "volume",
                "weight", "hole_size", "pressure", "temperature", "vertical_depth",
                "radial_drift", "angular_drift"
            ]:
                try:
                    print(equip.attic["LENGTH"].units)
                    # units_column.append(param.attic["VALUES"].units)
                    # unit = equip.attic[f"{attr}"].units
                    unit = getattr(equip, f"{attr}", None)
                    units[attr] = unit
                except AttributeError:
                    units[attr] = None
        return units