import pyarrow.feather as feather

def read_feather(filepath):
    """Reads a Feather file and returns it as a PyArrow Table.

    Args:
        filepath (str): Path to the Feather file.

    Returns:
        pyarrow.Table: The Feather file content as a PyArrow Table.

    Example:
        feather_data = read_feather("/projectnb/rcsmetrics/accounting/data/scc/2016.feather")
        
        # Get the number of rows in the Feather file
        num_rows = feather_data.num_rows
        print(f"Number of rows: {num_rows}")
    """
    return feather.read_table(filepath)
