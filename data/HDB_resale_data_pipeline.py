"""
This pipeline script processes the HDB resale data in the following ways:
- Consolidates the multiple HDB resale data files into one CSV.
- Transforms the various columns into

The HDB resale data is available at the following URL:
https://data.gov.sg/datasets?agencies=Housing+%26+Development+Board+(HDB)&resultId=189

Please note that for the best results, the last CSV file needs to be updated regularly:
    `Resale flat prices based on registration date from Jan-2017 onwards.csv`
It can be downloaded from the above URL.

Please run this script from the data/ folder.
"""
import pandas as pd

transformed_csv_name = "resale_data.csv"

subfolder_name = "ResaleFlatPrices"
resale_flat_prices_1 = "Resale Flat Prices (Based on Approval Date), 1990 - 1999.csv"
resale_flat_prices_2 = "Resale Flat Prices (Based on Approval Date), 2000 - Feb 2012.csv"
resale_flat_prices_3 = "Resale Flat Prices (Based on Registration Date), From Mar 2012 to Dec 2014.csv"
resale_flat_prices_4 = "Resale Flat Prices (Based on Registration Date), From Jan 2015 to Dec 2016.csv"
resale_flat_prices_5 = "Resale flat prices based on registration date from Jan-2017 onwards.csv"

resale_flat_prices_list = [
    f"{subfolder_name}/{resale_flat_prices_1}",
    f"{subfolder_name}/{resale_flat_prices_2}",
    f"{subfolder_name}/{resale_flat_prices_3}",
    f"{subfolder_name}/{resale_flat_prices_4}",
    f"{subfolder_name}/{resale_flat_prices_5}"
    ]

def consolidate_dfs(resale_flat_prices_list):
    """
    This function consolidates the various resale flat price datasets into one DataFrame.

    For best results, the last CSV file:
        `Resale flat prices based on registration date from Jan-2017 onwards.csv`
    needs to be updated regularly.
    """
    consolidated_df = pd.DataFrame()

    # The 'remaining_lease' column only exists from 2015 onwards and can be calculated
    col_to_drop = ['remaining_lease']

    for csv_file in resale_flat_prices_list:
        resale_df = pd.read_csv(csv_file)
        resale_df.drop(resale_df.filter(col_to_drop), inplace=True, axis=1)

        consolidated_df = pd.concat([consolidated_df, resale_df], ignore_index=True)

    return consolidated_df


def transform_df(consolidated_df):
    """
    This function transforms the various data columns of the resale flat dataframe:
    - The first 'month' column is transformed to a date column, taking the 1st of the month.
    - The 'flat_model' column is standardized to ALL CAPS.
    """

    # Transform 'month' column to 'resale_date' column
    consolidated_df['resale_date'] = pd.to_datetime(consolidated_df['month'], format='%Y-%m')

    # Move the resale date column to the front and remove the month column
    resale_date = consolidated_df.pop('resale_date')
    consolidated_df.insert(0, 'resale_date', resale_date)
    consolidated_df.pop('month')

    # Make 'flat_model' column ALL CAPS
    consolidated_df['flat_model'] = consolidated_df['flat_model'].str.upper()

    return consolidated_df


consolidated_df = consolidate_dfs(resale_flat_prices_list)

transformed_df = transform_df(consolidated_df)

transformed_df.to_csv(transformed_csv_name)