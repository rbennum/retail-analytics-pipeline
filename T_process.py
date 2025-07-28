"""
The sole purpose of this file is to perform the T of the ELT process:
- Clean the data
- Transform the data into a fact table and dimension tables
"""

import pandas as pd

RAW_WAREHOUSE_FILE = 'data/warehouse.csv'
STG_FILE = 'data/warehouse_cleaned.csv'
FCT_SALES_FILE = 'data/fct_sales.csv'
DIM_PRODUCT_FILE = 'data/dim_product.csv'
DIM_CUSTOMER_FILE = 'data/dim_customer.csv'
DIM_DATE_FILE = 'data/dim_date.csv'

def create_staging():
    df_warehouse = pd.read_csv(RAW_WAREHOUSE_FILE, parse_dates=['InvoiceDate'])
    df_cleaned = (
        df_warehouse
        .rename(columns={
            'Invoice': 'invoice_id',
            'StockCode': 'product_id',
            'Description': 'product_description',
            'Quantity': 'order_amt',
            'InvoiceDate': 'purchase_date',
            'Price': 'product_price',
            'Customer ID': 'customer_id',
            'Country': 'customer_country'
        })
        .assign(
            customer_id=lambda x: x['customer_id'].astype('Int64')
        )
        .query('order_amt > 0')
        .loc[lambda x: x['customer_id'].notna()]
        .assign(customer_id=lambda x: x['customer_id'].astype('str'))
        .drop_duplicates()
    )
    df_cleaned.to_csv(STG_FILE, index=False)
    print('done: cleaning warehouse data')
    print(f'result saved in {STG_FILE}')

def create_marts():
    df_stg = pd.read_csv(STG_FILE, parse_dates=['purchase_date'])
    fct_sales = pd.DataFrame(
        data={
            'invoice_id': df_stg['invoice_id'],
            'product_id': df_stg['product_id'],
            'customer_id': df_stg['customer_id'],
            'date_id': df_stg['purchase_date'].dt.date,
            'order_amt': df_stg['order_amt'],
            'product_price': df_stg['product_price'],
            'purchase_timestamp': df_stg['purchase_date'],
        }
    )
    dim_product = (
        pd.DataFrame(
            data={
                'product_id': df_stg['product_id'],
                'product_description': df_stg['product_description']
            }
        )
        .drop_duplicates()
    )
    dim_customer = (
        pd.DataFrame(
            data={
                'customer_id': df_stg['customer_id'],
                'customer_country': df_stg['customer_country'],
            }
        )
        .drop_duplicates()
    )
    dim_date = (
        pd.DataFrame(
            data={
                'date_id': pd.date_range(
                    start=df_stg['purchase_date'].min().normalize(),
                    end=df_stg['purchase_date'].max().normalize(),
                    freq='D'
                ),
            }
        )
        .assign(
            year=lambda x: x['date_id'].dt.year,
            month=lambda x: x['date_id'].dt.month,
            day_of_week=lambda x: x['date_id'].dt.day_of_week,
            is_weekend=lambda x: x['day_of_week'].isin([5, 6])
        )
    )
    fct_sales.to_csv(FCT_SALES_FILE, index=False)
    dim_product.to_csv(DIM_PRODUCT_FILE, index=False)
    dim_customer.to_csv(DIM_CUSTOMER_FILE, index=False)
    dim_date.to_csv(DIM_DATE_FILE, index=False)
    print('done: creating marts')

if __name__ == '__main__':
    create_staging()
    create_marts()