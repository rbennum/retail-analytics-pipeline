"""
The sole purpose of this file is to perform the E and L of ELT:
- Extract data from all sheets in the source Excel file.
- Load the combined, RAW data into a single CSV file that acts as our warehouse table.
"""

import pandas as pd

SOURCE_FILE_PATH = 'data/online_retail.xlsx'
RAW_WAREHOUSE_PATH = 'data/warehouse.csv'

def load_warehouse():
    xls = pd.ExcelFile(SOURCE_FILE_PATH)
    all_sheets = []
    
    for sheet_name in xls.sheet_names:
        print(f'reading sheet: {sheet_name}')
        df = pd.read_excel(xls, sheet_name=sheet_name)
        all_sheets.append(df)

    raw_df = pd.concat(all_sheets)

    print(f'loading {len(raw_df)} raw rows to {RAW_WAREHOUSE_PATH}...')
    raw_df.to_csv(RAW_WAREHOUSE_PATH, index=False)
    print('load complete')


if __name__ == "__main__":
    load_warehouse()