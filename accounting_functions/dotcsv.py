import csv

def read_csv(filepath):
    """Reads a CSV file and returns its content via a generator.

    Args:
        filepath (str): Path to the CSV file.

    Yields:
        dict: A dictionary representing a row in the CSV file.

    Example:
        file_path = "/projectnb/rcsmetrics/accounting/data/scc/2016.csv"
        
        # Convert to a list and print the number of records
        data_list = list(read_csv(file_path))
        print(f"Number of records: {len(data_list)}")
    """
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row
