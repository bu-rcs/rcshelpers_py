from dotcsv import read_csv

# Example usage
file_path = "/projectnb/rcsmetrics/accounting/data/scc/2016.csv"

# Converting to a list and printing the number of records
data_list = list(read_csv(file_path))
print(f"Number of records: {len(data_list)}")
print(f"Record 0: {data_list[0]}")
