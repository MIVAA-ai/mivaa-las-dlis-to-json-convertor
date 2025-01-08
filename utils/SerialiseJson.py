import json
import numpy as np
from pydantic import BaseModel

class JsonSerializable:
    @staticmethod
    def to_json(obj, **kwargs):
        """
        Converts an object to a JSON string, ensuring all numpy types are serializable.

        Args:
            obj (any): The object to convert and serialize.
            **kwargs: Additional arguments to pass to `json.dumps`.

        Returns:
            str: The JSON string representation of the object.
        """

        def convert(item):
            try:
                if isinstance(item, BaseModel):  # Convert Pydantic model to dictionary
                    return item.dict()
                elif isinstance(item, dict):  # Recurse into dictionaries
                    return {k: convert(v) for k, v in item.items()}
                elif isinstance(item, list):  # Recurse into lists
                    return [convert(v) for v in item]
                elif isinstance(item, tuple):  # Convert tuples to lists
                    return [convert(v) for v in item]
                elif isinstance(item, np.integer):  # General numpy integer
                    return int(item)
                elif isinstance(item, np.floating):  # General numpy float
                    return float(item)
                elif isinstance(item, np.ndarray):  # Convert numpy arrays to lists
                    return item.tolist()
                elif isinstance(item, np.generic):  # Handle all other numpy scalars
                    return item.item()
                else:  # Return unchanged if not special
                    return item
            except Exception as e:
                print(f"Error converting item: {item} - {e}")
                raise

        # Recursively clean up the object
        serializable_obj = convert(obj)

        return json.dumps(serializable_obj, **kwargs)