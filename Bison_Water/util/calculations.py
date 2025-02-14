import pandas as pd
import numpy as np
import scipy.interpolate
from scipy.optimize import brentq
from util.preprocessing import find_closest_time
from IPython.display import display
from util.dataloader import select_calib_data


def estimate_sampling_interval(site_time_info): # still naive -- 
    # estimates the sampling interval of some data bu taking the average time delta between the samples
    # this does not consider a reality where there's a multimodal distribution of sampling intervals
    # ------------------------------------------------
    # site_time_info: the 'timestamp' column of the dataframes we've been using
    # output: (int) the average time in minutes between samples
    # ================================================
    site_time_info['timestamp_datetime'] = pd.to_datetime(site_time_info)
    site_time_info['delta_t'] = site_time_info['timestamp_datetime'].diff().dt.total_seconds() / 60
    avg_mins = max(1,int(site_time_info['delta_t'].mean())) # NOTE: assume base sampling rate will always be once per minute
    return avg_mins # in units of how many minutes per sample


def find_intersection_point(func1, func2, x_min, x_max):  # from Ryan
    def func_diff(x):
        return func1(x) - func2(x)
    try:
        x_intersect = brentq(func_diff, x_min, x_max)
        y_intersect = func1(x_intersect)
        return x_intersect, y_intersect
    except ValueError:
        return None


def calc_perc_BEP(df, df_pump_data): # from Ryan
    # Define the necessary column names
    flow_column = 'flow rate'          # Replace with your actual flow rate column name
    frequency_column = 'frequency'     # Replace with your actual frequency column name
    
    # Separate speed and efficiency lines
    speed_data = df_pump_data[df_pump_data['type'] == 'speed']
    efficiency_data = df_pump_data[df_pump_data['type'] == 'efficiency']

    # Get unique labels for speed and efficiency lines
    speed_lines = sorted(speed_data['label'].unique(), key=lambda x: float(x))
    efficiency_lines = [line for line in sorted(efficiency_data['label'].unique()) if 'triangle' not in line.lower()]

    # Create interpolation functions for each speed line
    speed_line_funcs = {}
    for label in speed_lines:
        line_data = speed_data[speed_data['label'] == label].sort_values('x')
        x_speed = line_data['x'].values
        y_speed = line_data['y'].values
        speed_line_funcs[label] = scipy.interpolate.interp1d(x_speed, y_speed, bounds_error=False, fill_value='extrapolate')

    # Create interpolation functions for each efficiency line
    efficiency_line_funcs = {}
    for label in efficiency_lines:
        line_data = efficiency_data[efficiency_data['label'] == label].sort_values('x')
        x_efficiency = line_data['x'].values
        y_efficiency = line_data['y'].values
        efficiency_line_funcs[label] = scipy.interpolate.interp1d(x_efficiency, y_efficiency, bounds_error=False, fill_value='extrapolate')

    # Find intersection points between speed lines and efficiency lines
    intersection_points = {}

    for speed_line in speed_lines:
        speed_func = speed_line_funcs[speed_line]
        x_speed_min = speed_data[speed_data['label'] == speed_line]['x'].min()
        x_speed_max = speed_data[speed_data['label'] == speed_line]['x'].max()
        intersection_points[speed_line] = {}
        for efficiency_line in efficiency_lines:
            eff_func = efficiency_line_funcs[efficiency_line]
            x_eff_min = efficiency_data[efficiency_data['label'] == efficiency_line]['x'].min()
            x_eff_max = efficiency_data[efficiency_data['label'] == efficiency_line]['x'].max()
            x_min = max(x_speed_min, x_eff_min)
            x_max = min(x_speed_max, x_eff_max)
            if x_min < x_max:
                intersection = find_intersection_point(speed_func, eff_func, x_min, x_max)
                if intersection is not None:
                    intersection_points[speed_line][efficiency_line] = intersection
                else:
                    print(f"No intersection found between speed line {speed_line} and efficiency line {efficiency_line}")
            else:
                print(f"No overlapping x range between speed line {speed_line} and efficiency line {efficiency_line}")

    # Interpolate between Min to BEP and BEP to Max for each speed line
    interpolated_speed_line = {}

    for speed_line in speed_lines:
        if all(key in intersection_points[speed_line] for key in ['Min', 'BEP', 'Max']):
            min_point = intersection_points[speed_line]['Min']
            bep_point = intersection_points[speed_line]['BEP']
            max_point = intersection_points[speed_line]['Max']
        else:
            print(f"Missing intersection points for speed line {speed_line}, skipping")
            continue

        speed_func = speed_line_funcs[speed_line]

        # Interpolate between Min and BEP
        x_min_bep = np.linspace(min_point[0], bep_point[0], 100)
        y_min_bep = speed_func(x_min_bep)
        health_score_min_bep = np.linspace(-100, 0, 100)

        # Interpolate between BEP and Max
        x_bep_max = np.linspace(bep_point[0], max_point[0], 100)
        y_bep_max = speed_func(x_bep_max)
        health_score_bep_max = np.linspace(0, 100, 100)

        # Combine the two
        x_interp = np.concatenate([x_min_bep, x_bep_max])
        y_interp = np.concatenate([y_min_bep, y_bep_max])
        health_score = np.concatenate([health_score_min_bep, health_score_bep_max])

        interpolated_speed_line[speed_line] = pd.DataFrame({
            'flow_rate': x_interp,
            'tdh': y_interp,
            'perc_from_BEP': health_score
        })

    # Create interpolation functions for each speed line
    health_score_functions = {}
    for speed_line in interpolated_speed_line:
        df_line = interpolated_speed_line[speed_line]
        df_line = df_line.drop_duplicates(subset='flow_rate').sort_values('flow_rate')
        flow_rate = df_line['flow_rate'].values
        health_score = df_line['perc_from_BEP'].values
        health_score_func = scipy.interpolate.interp1d(
            flow_rate, health_score, bounds_error=False, fill_value=(health_score[0], health_score[-1])
        )
        health_score_functions[speed_line] = health_score_func

    # Extract numeric frequencies from speed lines
    speed_line_freqs = np.array([float(speed_line) for speed_line in speed_lines])

    # Function to compute health score for each time sample
    def compute_health_score(row):
        f = row[frequency_column]
        q = row[flow_column]

        # Check if frequency is within the desired range
        if f < freq_min or f > freq_max:
            return np.nan  # Exclude frequencies outside the range

        # Find two nearest speed lines
        freq_array = speed_line_freqs
        if f <= freq_array.min():
            f1 = f2 = freq_array.min()
        elif f >= freq_array.max():
            f1 = f2 = freq_array.max()
        else:
            idx = np.searchsorted(freq_array, f)
            f1 = freq_array[idx - 1]
            f2 = freq_array[idx]

        f1_str = str(int(f1)) if f1 == int(f1) else str(f1)
        f2_str = str(int(f2)) if f2 == int(f2) else str(f2)

        # Compute weights
        if f1 == f2:
            weight1 = 1.0
            weight2 = 0.0
        else:
            weight1 = (f2 - f) / (f2 - f1)
            weight2 = (f - f1) / (f2 - f1)

        # Get health scores at flow rate q
        try:
            health_score_f1 = health_score_functions[f1_str](q)
        except KeyError:
            print(f"Speed line {f1_str} not found")
            return np.nan
        except ValueError:
            return np.nan  # Flow rate q is outside interpolation range
        try:
            health_score_f2 = health_score_functions[f2_str](q)
        except KeyError:
            print(f"Speed line {f2_str} not found")
            return np.nan
        except ValueError:
            return np.nan  # Flow rate q is outside interpolation range

        # Compute final health score
        health_score = weight1 * health_score_f1 + weight2 * health_score_f2
        return health_score

    # Filter the DataFrame to only include frequencies between the lowest and highest speed lines
    freq_min = speed_line_freqs.min()
    freq_max = speed_line_freqs.max()
    df = df[(df[frequency_column] >= freq_min) & (df[frequency_column] <= freq_max)]

    # Apply the function to compute health score for each row
    df['perc_from_BEP'] = df.apply(compute_health_score, axis=1)

    # Remove rows with NaN health scores (which may result from frequencies outside the range)
    df = df.dropna(subset=['perc_from_BEP'])

    return df


def interpolate_missing_freqs(freq_dict): # From Ryan
        """
        Interpolates means and stds for missing frequencies.
        freq_dict should be {freq: {'mean':..., 'std':...}}
        """
        freqs = sorted(freq_dict.keys())
        if len(freqs) <= 1:
            # If we have only one or zero frequencies, no interpolation is possible
            return freq_dict

        full_freq_range = range(freqs[0], freqs[-1] + 1)
        interpolated_freqs = set()
        for f in full_freq_range:
            if f not in freq_dict:
                # Find lower and higher available frequencies
                lower_freqs = [ff for ff in freqs if ff < f]
                higher_freqs = [ff for ff in freqs if ff > f]
                if not lower_freqs or not higher_freqs:
                    # Cannot interpolate if no lower or higher freq exists
                    continue
                lower_f = max(lower_freqs)
                higher_f = min(higher_freqs)
                # Linear interpolation of mean and std
                w = (f - lower_f) / (higher_f - lower_f)
                mean_val = freq_dict[lower_f]['mean'] + w * (freq_dict[higher_f]['mean'] - freq_dict[lower_f]['mean'])
                std_val = freq_dict[lower_f]['std'] + w * (freq_dict[higher_f]['std'] - freq_dict[lower_f]['std'])
                freq_dict[f] = {
                    'mean': mean_val,
                    'std': std_val
                }
                interpolated_freqs.add(int(f))

        return freq_dict, interpolated_freqs


def normalize_BEP(df, calibration_stages,approx_time=True):
    # for each frequency that is calibrated (nearest whole), find the mean and standard deviation of the KPI to normalize (kWh/BBL)
    # for each frequency that is not being calibrated (nearest whole), approximate it based on interpolated data
    # then for each datapoint, consider its frequency, then normalize the KPI (kWh/BBL) by its calibration statistics (mean and std)
    # NOTE: only feed in df that has undergone previous BEP calc stage, and rounding of frequencies to nearest int
    # ------------------------------------------------
    # df: dataframe with Bison Data
    # calibration_stages: Ryan's sites_info format has the calibration stage information stored in a specific way, and all sites' calibration stage info is computed here
    # approx_time: if the the exact start/end time can't be found in the dataframe, find the closest time to it
    # output: None
    # ================================================
    
    health_score_column = 'perc_from_BEP'
    normalized_health_score_column = 'norm_perc_from_BEP'

    # Compute mean and std from calibration intervals
    freq_dict = {}
    for stage in calibration_stages:
        freq = stage['frequency']
        freq_cal_data = select_calib_data(df, stage,approx_time=approx_time)       
        freq_mean = freq_cal_data[health_score_column].mean() # if not freq_cal_data.empty else np.nan 
        freq_std = freq_cal_data[health_score_column].std() #  if not freq_cal_data.empty else np.nan
        # print("freq_mean: {}, freq_std: {}".format(freq_mean, freq_std))
        freq_dict[freq] = {
            'mean': freq_mean,
            'std': freq_std
        }

    # Interpolate missing frequencies if needed
    freq_dict, interpolated_freqs = interpolate_missing_freqs(freq_dict)

    # NOTE: Audrey had to modify the normalization functionality in a way where data structures were accessed differently 
    # Normalize the health score -- use if frequencies have NOT been snapped to ints
    unnormalized_freqs = set()
    normalized_freqs = set()
    df[normalized_health_score_column] = pd.Series([np.nan] * len(df[health_score_column]))
    NORMALIZED_X_ROWS = 0

    for i,row in df.iterrows():
        if i == len(df): #... is there an out of bounds error here? we index - 0 at i...
            break
        f = row['frequency'] 
        if f in freq_dict: # this frequency found in the data WAS calibrated
            NORMALIZED_X_ROWS += 1
            mean = freq_dict[f]['mean']
            std = freq_dict[f]['std']
            if std != 0:
                norm_BEP = (row[health_score_column] - mean) / std
                df.loc[i,normalized_health_score_column] = norm_BEP # using df.iloc[row][col] 1) returns a COPY 2) sets the COPY -- do it like this, or even use .at[]
                    # https://stackoverflow.com/questions/76416898/pandas-doesnt-assign-values-to-dataframe
            normalized_freqs.add(int(f))
        else:
            unnormalized_freqs.add(int(f))

    # print("NORMALIZED_X_ROWS: {} out of {}; {}%".format(NORMALIZED_X_ROWS, i, round(NORMALIZED_X_ROWS/i,3)*100))
    return df, unnormalized_freqs, interpolated_freqs, normalized_freqs


def calc_kWh_BBL(df): # from Ryan
    # Ensure 'timestamp' is in datetime format
    df['timestamp_datetime'] = pd.to_datetime(df['timestamp'])
    df['delta_t'] = df['timestamp_datetime'].diff()

    # Apply initial filter for valid samples before feature derivations
    initial_valid = (
        df['frequency'].notna() &
        df['amps'].notna() &
        df['volts'].notna() &
        df['flow rate'].notna() &
        (df['flow rate'] < 40000)
    )

    # =========================
    # Identify Invalid Samples and Their Neighbors
    # =========================

    # 1. Identify invalid samples based on initial_valid
    invalid = ~initial_valid
    # 2. Identify samples immediately before invalid samples
    invalid_prev = invalid.shift(1).fillna(False)
    # 3. Identify samples immediately after invalid samples
    invalid_next = invalid.shift(-1).fillna(False)
    # 4. Define the updated validity mask:
    #    A sample is valid only if:
    #    - It meets the initial_valid conditions, AND
    #    - Neither the previous nor the next sample is invalid
    initial_valid = initial_valid & ~invalid_prev & ~invalid_next

    # Filter the DataFrame
    df = df[initial_valid].copy()

    # Compute power in kilowatts
    pf = 1.0 #power factor changes by motors
    df['power_kW'] = (pf*np.sqrt(3)*df['volts'] * df['amps']) / 1000

    # Calculate energy consumed during each interval in kilowatt-hours
    df['energy_kWh'] = df['power_kW'] * df['delta_t'].dt.total_seconds() / 3600

    # Calculate volume pumped during each interval in barrels
    df['volume_bbl'] = df['flow rate'] * df['delta_t'].dt.total_seconds() / 86400
    df['volume_bbl_fr'] = df['flow rate'] * df['delta_t'].dt.total_seconds() / 86400

    # Create a mask for valid calculations
    valid = df['delta_t'].notna() & (df['delta_t'].dt.total_seconds() > 0)
    valid &= (df['volume_bbl'] > 0)

    # Compute energy per barrel in kWh/bbl
    df['kWh/BBL'] = np.nan
    df.loc[valid, 'kWh/BBL'] = df.loc[valid, 'energy_kWh'] / df.loc[valid, 'volume_bbl']
    return df