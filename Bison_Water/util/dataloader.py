import os
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from util.preprocessing import process_voltage_and_current, find_closest_time
import json


def site_ids_name(): # hardcoded TODO: load this in from somewhere
    ids = {}
    ids[33404] = "Union City" 
    ids[33467] = "Siegrist"
    ids[57740] = "Canadian"
    ids[33614] = "Calument"
    return ids


def load_pump_curves(site_ids):
    datapath = "/Users/audreyder/Neuralix/AllPumpCSV/" 
    pump_curves = {site_id:None for site_id in site_ids}
    calumet = pd.read_csv(datapath+"PumpCurve_Calument_33614_DataPoints.csv")
    canadian = pd.read_csv(datapath+"PumpCurve_Canadian_57740_DataPoints.csv")
    siegrist = pd.read_csv(datapath+"PumpCurve_Siegrist_33467_DataPoints.csv")
    unioncity = pd.read_csv(datapath+"PumpCurve_UnionCity2_33404_DataPoints.csv")
    pump_curves[33404] = unioncity # hardcoded TODO: Future proof; do this dynamically by site_id
    pump_curves[33467] = siegrist
    pump_curves[57740] = canadian
    pump_curves[33614] = calumet
    return pump_curves


def cached_bison_data(filepath):
    df = pd.read_csv(filepath)
    print("Data columns: {}".format(df.columns))
    print("Number of rows: {}".format(len(df)))
    print("Earliest timestamp: {}".format(df.iloc[0]['timestamp']))
    print("Latest timestamp: {}".format(df.iloc[-1]['timestamp']))
    return df


def fetch_bison_data(filepath, start_time=datetime.now() - timedelta(days=90), end_time=datetime.now()):
    device_ids = [
        57740,
        # 48034,
        33614,
        33404,
        33467,    
        48137,
        48138
    ]  # List of device IDs (integers), e.g., [57740, 33614]
    device_name_substrings = []#['canadian']  # List of devicename substrings, e.g., ['canadian', 'alpha']
    
    # 2. Time Zone
    time_zone = 'UTC'

    # 3. Measurement Mappings
    measurements = {
        'Vibration': ['sv/pmp_vib_rear', 'sv/hp1_vib', 'sv/vib'],
        'Thrust Temperature': ['sv/thrust_temp'],
        'Suction Pressure': ['sv/suctp'],
        'Discharge Pressure': ['sv/discp'],
        'Flow Rate': ['sv/fr'],
        'Frequency': ['sv/vfd_speed', 'sv/HZ', 'sv/hp1_hz'],
        'Amps': ['sv/vfd_current','sv/hp1_amps_a'],
        'AmpsA': ['sv/hp1_amps_a','sv/uphase'],
        'AmpsB': ['sv/hp1_amps_b','sv/vphase'],
        'AmpsC': ['sv/hp1_amps_c','sv/wphase'],
        'Volts': ['sv/vfd_voltage', 'sv/outv'],
        'VoltsA': ['sv/hp1_volts_a'],
        'VoltsB': ['sv/hp1_volts_b'],
        'VoltsC': ['sv/hp1_volts_c'],
        'Meter Total': ['sv/accum_volume', 'sv/av'],
        # Add more measurements as needed
    }

    # 4. Time Range
    # time_interval = '1 hour'  # Adjust as needed
    time_interval = None

    # Function to generate SQL query
    def generate_sql_query(device_ids, device_name_substrings, time_zone, measurements, time_interval):
        # Build SELECT clauses for each measurement
        select_clauses = [
            "h.deviceid AS site_id",
            "d.devicename AS facility_name",
            "CONVERT_TIMEZONE(%s, h.datetime) AS timestamp"
        ]
        params = [time_zone]
        where_paths = set()

        for measurement_name, paths in measurements.items():
            case_conditions = "\n            ".join(
                [f"WHEN h.path = '{path}' THEN h.value" for path in paths]
            )
            select_clause = f"""MAX(
                    CASE
                        {case_conditions}
                        ELSE NULL
                    END
                ) AS "{measurement_name}" """
            select_clauses.append(select_clause)
            where_paths.update(paths)

        # Build WHERE clause components
        where_clauses = []
        where_params = []

        # Device ID filtering
        if device_ids:
            device_id_placeholders = ', '.join(['%s'] * len(device_ids))
            where_clauses.append(f"h.deviceid IN ({device_id_placeholders})")
            where_params.extend(device_ids)

        # Device name substring filtering
        if device_name_substrings:
            name_conditions = []
            for substring in device_name_substrings:
                name_conditions.append("d.devicename ILIKE %s")
                where_params.append(f"%{substring}%")
            where_clauses.append("(" + " OR ".join(name_conditions) + ")")

        # Only include devices that are not disabled
        where_clauses.append("d.disabled = %s")
        where_params.append(False)

        # h.path filtering
        path_placeholders = ', '.join(['%s'] * len(where_paths))
        where_clauses.append(f"h.path IN ({path_placeholders})")
        where_params.extend(where_paths)

        # Time filtering
        # where_clauses.append(f"h.datetime > GETDATE() - INTERVAL %s")
        # where_params.append(time_interval)
        # Alternatively, for specific times:
        where_clauses.append(f"h.datetime BETWEEN %s AND %s")
        where_params.append(start_time)
        where_params.append(end_time)

        # Combine WHERE clause
        where_clause = "WHERE\n    " + "\n    AND ".join(where_clauses)

        # Assemble the final query
        query = f"""
    SELECT
        {',    '.join(select_clauses)}
    FROM
        historical AS h
        JOIN devices AS d ON h.deviceid = d.deviceid
    {where_clause}
    GROUP BY
        h.deviceid, d.devicename, h.datetime
    ORDER BY
        h.deviceid, h.datetime;
    """
        # Combine all parameters
        params.extend(where_params)
        return query, params

    # Generate the query and parameters
    query, params = generate_sql_query(device_ids, device_name_substrings, time_zone, measurements, time_interval)
    
    cred_path = "/Users/audreyder/Neuralix/bison_credentials.json"
    credentials = None
    with open(cred_path, 'r') as file:
        credentials = json.load(file)
    REDSHIFT_ENDPOINT = credentials['REDSHIFT_ENDPOINT']
    REDSHIFT_PORT = credentials['REDSHIFT_PORT']
    REDSHIFT_DBNAME = credentials['REDSHIFT_DBNAME']
    REDSHIFT_USER = credentials['REDSHIFT_USER']
    REDSHIFT_PASS = credentials['REDSHIFT_PASS']

    conn = psycopg2.connect(
        user = REDSHIFT_USER,
        password = REDSHIFT_PASS,
        host = REDSHIFT_ENDPOINT,
        port = REDSHIFT_PORT,
        database = REDSHIFT_DBNAME
    )

    # Execute the query and load results into a pandas DataFrame
    df = pd.read_sql_query(query, conn, params=params)
    df['pump_id'] = 1

    # Close the database connection
    conn.close()
    df.set_index(['site_id','pump_id','timestamp'],inplace=True)

    # Process the voltage and current columns
    df = process_voltage_and_current(df)
    df.to_csv(filepath, index=True)
    return df


def cached_site_info(dict=False):
    # UPDATED SITES INFO - 01-24-2025
    # updated Calumet Calibration stage estimations for ground truth
    # NOTE: these estimations of ground truth are based on strict assumptions each ...
    # ...frequency is tested for precisely two hours (and not a half an hour more)
    # in the future, we are working on making this more sophisticated

    if dict:
        sites_info = {33614:
        {'enable': False, 'site_name': 'Calumet SWD', 'site_id': 33614, 'num_pumps': 1, 'site_dir_name': 'CalumetSWD(33614)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_Calument_33614_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 48, 'start_time': '2025-01-15 15:00:19', 'end_time': '2025-01-15 16:20:19'}, 
            {'frequency': 50, 'start_time': '2025-01-15 16:25:19', 'end_time': '2025-01-15 18:10:17'}, 
            {'frequency': 52, 'start_time': '2025-01-15 18:15:19', 'end_time': '2025-01-15 19:50:23'}, 
            {'frequency': 54, 'start_time': '2025-01-15 19:55:18', 'end_time': '2025-01-15 21:40:18'}, 
            {'frequency': 56, 'start_time': '2025-01-15 21:45:18', 'end_time': '2025-01-15 23:35:20'}, 
            {'frequency': 58, 'start_time': '2025-01-15 23:40:19', 'end_time': '2025-01-16 00:08:30'}]}, 
        57740:
        {'enable': False, 'site_name': 'Canadian SWD', 'site_id': 57740, 'num_pumps': 1, 'site_dir_name': 'CanadianSWD(57740)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_Canadian_57740_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 44, 'start_time': '2024-12-13 12:00:02', 'end_time': '2024-12-13 13:58:02'}, 
            {'frequency': 47, 'start_time': '2024-12-13 13:59:03', 'end_time': '2024-12-13 15:51:03'}, 
            {'frequency': 49, 'start_time': '2024-12-13 15:52:03', 'end_time': '2024-12-13 17:51:03'}, 
            {'frequency': 51, 'start_time': '2024-12-13 17:52:02', 'end_time': '2024-12-13 19:50:04'}, 
            {'frequency': 53, 'start_time': '2024-12-13 19:51:03', 'end_time': '2024-12-13 21:47:04'}, 
            {'frequency': 55, 'start_time': '2024-12-13 21:48:03', 'end_time': '2024-12-13 23:53:02'}, 
            {'frequency': 57, 'start_time': '2024-12-13 23:54:03', 'end_time': '2024-12-14 01:53:03'}, 
            {'frequency': 59, 'start_time': '2024-12-14 01:54:03', 'end_time': '2024-12-14 03:53:02'}]}, 
        33467:
        {'enable': False, 'site_name': 'Siegrist SWD', 'site_id': 33467, 'num_pumps': 1, 'site_dir_name': 'SiegristSWD(33467)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_Siegrist_33467_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 47, 'start_time': '2024-12-17 10:00:19', 'end_time': '2024-12-17 12:00:21'}, 
            {'frequency': 49, 'start_time': '2024-12-17 12:05:25', 'end_time': '2024-12-17 14:00:32'}, 
            {'frequency': 51, 'start_time': '2024-12-17 14:01:18', 'end_time': '2024-12-17 16:00:19'}, 
            {'frequency': 53, 'start_time': '2024-12-17 16:01:04', 'end_time': '2024-12-17 18:00:38'}, 
            {'frequency': 55, 'start_time': '2024-12-17 18:00:58', 'end_time': '2024-12-17 20:00:22'}, 
            {'frequency': 57, 'start_time': '2024-12-17 20:05:27', 'end_time': '2024-12-17 21:55:19'}]}, 
        33404:
        {'enable': False, 'site_name': 'Union City 2 SWD', 'site_id': 33404, 'num_pumps': 1, 'site_dir_name': 'UnionCity2SWD(33404)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_UnionCity2_33404_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 46, 'start_time': '2024-12-17 12:00:12', 'end_time': '2024-12-17 13:50:12'}, 
            {'frequency': 48, 'start_time': '2024-12-17 13:55:11', 'end_time': '2024-12-17 15:50:11'}, 
            {'frequency': 50, 'start_time': '2024-12-17 15:55:11', 'end_time': '2024-12-17 17:50:12'}, 
            {'frequency': 52, 'start_time': '2024-12-17 17:55:12', 'end_time': '2024-12-17 19:50:11'}, 
            {'frequency': 54, 'start_time': '2024-12-17 19:55:11', 'end_time': '2024-12-17 21:45:11'}, 
            {'frequency': 56, 'start_time': '2024-12-17 21:50:11', 'end_time': '2024-12-17 23:35:14'}]},
        48137:
        {'enable': False, 'site_name': '1509 1 SWD', 'site_id': 48137, 'num_pumps': 1, 'site_dir_name': '', 
        'pump_curve_path': "1509 SWD",
        'calibration_stages': []},

        48138:
        {'enable': False, 'site_name': '1509 2 SWD', 'site_id': 48138, 'num_pumps': 1, 'site_dir_name': '', 
        'pump_curve_path': "1509 SWD",
        'calibration_stages': []}
            
        }

    else:
        sites_info = [
        {'enable': False, 'site_name': 'Calumet SWD', 'site_id': 33614, 'num_pumps': 1, 'site_dir_name': 'CalumetSWD(33614)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_Calument_33614_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 48, 'start_time': '2025-01-15 15:00:19', 'end_time': '2025-01-15 16:20:19'}, 
            {'frequency': 50, 'start_time': '2025-01-15 16:25:19', 'end_time': '2025-01-15 18:10:17'}, 
            {'frequency': 52, 'start_time': '2025-01-15 18:15:19', 'end_time': '2025-01-15 19:50:23'}, 
            {'frequency': 54, 'start_time': '2025-01-15 19:55:18', 'end_time': '2025-01-15 21:40:18'}, 
            {'frequency': 56, 'start_time': '2025-01-15 21:45:18', 'end_time': '2025-01-15 23:35:20'}, 
            {'frequency': 58, 'start_time': '2025-01-15 23:40:19', 'end_time': '2025-01-16 00:08:30'}]}, 

        {'enable': False, 'site_name': 'Canadian SWD', 'site_id': 57740, 'num_pumps': 1, 'site_dir_name': 'CanadianSWD(57740)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_Canadian_57740_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 44, 'start_time': '2024-12-13 12:00:02', 'end_time': '2024-12-13 13:58:02'}, 
            {'frequency': 47, 'start_time': '2024-12-13 13:59:03', 'end_time': '2024-12-13 15:51:03'}, 
            {'frequency': 49, 'start_time': '2024-12-13 15:52:03', 'end_time': '2024-12-13 17:51:03'}, 
            {'frequency': 51, 'start_time': '2024-12-13 17:52:02', 'end_time': '2024-12-13 19:50:04'}, 
            {'frequency': 53, 'start_time': '2024-12-13 19:51:03', 'end_time': '2024-12-13 21:47:04'}, 
            {'frequency': 55, 'start_time': '2024-12-13 21:48:03', 'end_time': '2024-12-13 23:53:02'}, 
            {'frequency': 57, 'start_time': '2024-12-13 23:54:03', 'end_time': '2024-12-14 01:53:03'}, 
            {'frequency': 59, 'start_time': '2024-12-14 01:54:03', 'end_time': '2024-12-14 03:53:02'}]}, 

        {'enable': False, 'site_name': 'Siegrist SWD', 'site_id': 33467, 'num_pumps': 1, 'site_dir_name': 'SiegristSWD(33467)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_Siegrist_33467_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 47, 'start_time': '2024-12-17 10:00:19', 'end_time': '2024-12-17 12:00:21'}, 
            {'frequency': 49, 'start_time': '2024-12-17 12:05:25', 'end_time': '2024-12-17 14:00:32'}, 
            {'frequency': 51, 'start_time': '2024-12-17 14:01:18', 'end_time': '2024-12-17 16:00:19'}, 
            {'frequency': 53, 'start_time': '2024-12-17 16:01:04', 'end_time': '2024-12-17 18:00:38'}, 
            {'frequency': 55, 'start_time': '2024-12-17 18:00:58', 'end_time': '2024-12-17 20:00:22'}, 
            {'frequency': 57, 'start_time': '2024-12-17 20:05:27', 'end_time': '2024-12-17 21:55:19'}]}, 
            
        {'enable': False, 'site_name': 'Union City 2 SWD', 'site_id': 33404, 'num_pumps': 1, 'site_dir_name': 'UnionCity2SWD(33404)', 
        'pump_curve_path': '/Users/audreyder/Neuralix/AllPumpCSV/PumpCurve_UnionCity2_33404_DataPoints.csv', # replace path as appropriate
        'calibration_stages': [
            {'frequency': 46, 'start_time': '2024-12-17 12:00:12', 'end_time': '2024-12-17 13:50:12'}, 
            {'frequency': 48, 'start_time': '2024-12-17 13:55:11', 'end_time': '2024-12-17 15:50:11'}, 
            {'frequency': 50, 'start_time': '2024-12-17 15:55:11', 'end_time': '2024-12-17 17:50:12'}, 
            {'frequency': 52, 'start_time': '2024-12-17 17:55:12', 'end_time': '2024-12-17 19:50:11'}, 
            {'frequency': 54, 'start_time': '2024-12-17 19:55:11', 'end_time': '2024-12-17 21:45:11'}, 
            {'frequency': 56, 'start_time': '2024-12-17 21:50:11', 'end_time': '2024-12-17 23:35:14'}]}]

    return sites_info


def select_calib_data(df, stage, approx_time=True):
    # for a given calibration stage and the dataframe that data is in, select just that calibration data
    # ------------------------------------------------
    # df: the dataframe the calibration stage data is in
    # stage: the calibration stage data for a given site in the Ryan sites_info format (see cached_site_info())
    # output: the calibration data for the input stage specified
    # ================================================
    start = stage['start_time']
    end = stage['end_time']

    start_idx, end_idx = None, None
        # first condition: do you find the exact timestamp in the db? second condition: do you allow searching for the closest approx time? or do you want exact?
    if len(df[df['timestamp']==start]) == 0 and approx_time:
        date_time = start.split(" ")
        if approx_time:
            start, start_idx = find_closest_time(df,date_time[0],query_time=date_time[1])
    else:
        start_idx = df[df['timestamp']==start].index[0]
        
    if len(df[df['timestamp']==end]) == 0 and approx_time:
        date_time = end.split(" ")
        end, end_idx = find_closest_time(df,date_time[0],query_time=date_time[1])
    else:
        end_idx = df[df['timestamp']==end].index[0]
    
    # datapoints for the frequency being calibrated at this stage -- TODO: confirm, this should never be np.nan...?
    stage_cal_data = df.loc[start_idx:end_idx]
    return stage_cal_data