from dotfeather import read_feather

# Example usage
file_path = "/projectnb/rcsmetrics/accounting/data/scc/2016.feather"

# Printing the number of records
data_list = read_feather(file_path)
print(f"Number of records: {data_list.num_rows}")
print(f"Record 0: {data_list.slice(0, 1)}")
