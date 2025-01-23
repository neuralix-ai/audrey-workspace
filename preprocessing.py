import os
import pandas as pd



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

    freq_min = 40
    freq_max = 60

    # Apply filtering masks on the main DataFrame (df)
    mask = (df[frequency_float_column].astype(float) >= freq_min) & (df[frequency_float_column].astype(float) <= freq_max)
    mask &= df[flow_column].astype(float) < 30000
    mask &= df[flow_column].astype(float) > 5000
    # If needed:
    # mask &= df[pressure_column].astype(float) > 1300

    df = df[mask]

    df[frequency_int_column] = df[frequency_float_column].round().astype('Int64')
    return df


# def threshold_filtering(df):
#     # If you need to ensure 'timestamp' is a datetime, reassign the index:
#     df.index = pd.MultiIndex.from_arrays(
#         [
#             df.index.get_level_values('site_id'),
#             df.index.get_level_values('pump_id'),
#             pd.to_datetime(df.index.get_level_values('timestamp'))
#         ],
#         names=['site_id', 'pump_id', 'timestamp']
#     )

#     # If you need a date filter, you can do something like:
#     # start_date = '2024-10-10'
#     # end_date = df.index.get_level_values('timestamp').max()
#     # df = df.loc[(slice(None), slice(None), slice(start_date, end_date)), :]

#     # Define pump-related variables
#     pressure_column = 'discharge pressure'
#     flow_column = 'flow rate'
#     frequency_float_column = 'frequency'
#     frequency_int_column = 'frequency int'

#     freq_min = 40
#     freq_max = 60

#     # Apply filtering masks on the main DataFrame (df)
#     mask = (df[frequency_float_column].astype(float) >= freq_min) & (df[frequency_float_column].astype(float) <= freq_max)
#     mask &= df[flow_column].astype(float) < 30000
#     mask &= df[flow_column].astype(float) > 5000
#     # If needed:
#     # mask &= df[pressure_column].astype(float) > 1300

#     df = df[mask]

#     df[frequency_int_column] = df[frequency_float_column].round().astype('Int64')
#     return df