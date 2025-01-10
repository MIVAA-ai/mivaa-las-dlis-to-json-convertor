import pandas as pd


def summary_dataframe(items, **kwargs):
    """
    Converts a list of items into a DataFrame with specified attributes.

    Args:
        items (list): List of items to convert.
        kwargs: Keyword arguments where keys are item attributes and values are column names.

    Returns:
        pd.DataFrame: A DataFrame containing the specified attributes.
    """
    data = {value: [] for value in kwargs.values()}

    for item in items:
        for key, column_name in kwargs.items():
            try:
                data[column_name].append(getattr(item, key, None))
            except Exception as e:
                print(f"Error processing attribute '{key}' for item '{item}': {e}")
                data[column_name].append(None)

    return pd.DataFrame(data)