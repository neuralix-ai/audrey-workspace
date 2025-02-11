import os
import pandas as pd
from datetime import datetime


def find_closest_time(data, query_date, query_time='00:00:00'):
    # find the closest entry to the query date and time in the dataframe
    # ------------------------------------------------
        # data: dataframe of the site you're looking at
        # query_date: %Y-%m-%d format
        # query time: %H:%M:%S format
        # output: closest entry in the dataframe timestamp (datetime object), its corresponding dataframe index (int)
    # ================================================
    date_subset = data[data['timestamp'].str.contains(query_date)]
    query_datetime = datetime.strptime(query_date+" "+query_time, "%Y-%m-%d %H:%M:%S")
    closest_timestamp = datetime.strptime(date_subset.iloc[0]['timestamp'], "%Y-%m-%d %H:%M:%S")
    closest_timeidx = date_subset.iloc[0].name

    for i, row in date_subset.iterrows():
        curr_date = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
        if abs(query_datetime-curr_date) < abs(query_datetime-closest_timestamp):
            closest_timestamp = curr_date
            closest_timeidx = i
    return closest_timestamp, closest_timeidx # errs on the side of being later


# =======================================================================================

# Code originally from Ryan Mercer; ported (01/15/2025) and cleaned here
# Function to process voltage and current columns
def process_voltage_and_current(df): # syncdatabase_011525 may not have this run
    # Handle voltage columns
    voltage_cols = ['volts', 'voltsa', 'voltsb', 'voltsc']
    if all(col in df.columns for col in voltage_cols):
        df['volts'] = df[voltage_cols].mean(axis=1, skipna=True)

    # Handle current columns
    current_cols = ['amps', 'ampsa', 'ampsb', 'ampsc']
    if all(col in df.columns for col in current_cols):
        df['amps'] = df[current_cols].mean(axis=1, skipna=True)

    return df


def remove_NaN_cols(df):
    nan_col_bool = df.isna().all()
    nan_cols = []
    for i,col in enumerate(df.columns):
        if nan_col_bool[i] == True:
            nan_cols.append(col)
    print(nan_cols)
    df.drop(nan_cols, axis=1, inplace=False)
    return df


def threshold_filtering(df):
    # Define pump-related variables
    pressure_column = 'discharge pressure'
    flow_column = 'flow rate'
    frequency_float_column = 'frequency'
    frequency_int_column = 'frequency int'

    freq_min = 30
    freq_max = 65

    # Apply filtering masks on the main DataFrame (df)
    mask = (df[frequency_float_column].astype(float) >= freq_min) & (df[frequency_float_column].astype(float) <= freq_max)
    mask &= df[flow_column].astype(float) < 30000
    mask &= df[flow_column].astype(float) > 5000
    # If needed:
    # mask &= df[pressure_column].astype(float) > 1300

    df = df[mask]

    df[frequency_int_column] = df[frequency_float_column].round().astype('Int64')
    return df