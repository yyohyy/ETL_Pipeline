import pandas as pd
import os
from library.variables import Variables

csv_dir_path = Variables("ETL").get_variable().get("csv_dir_path")

def create_csv(data, column_names, file_name):
    try:
        file_path = f"{csv_dir_path}/{file_name}.csv"
        df = pd.DataFrame(data, columns=column_names)
        df.to_csv(file_path, index=False, encoding="utf-8")
        print(f"Data successfully exported to '{file_name}'.")
    except Exception as e:
        print(f"Error exporting data to CSV: {e}")

def delete_csv(file_name):
    file_path = f"{csv_dir_path}/{file_name}.csv"
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        raise Exception(f"An error occurred while deleting the CSV file: {e}")