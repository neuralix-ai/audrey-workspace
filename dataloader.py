import os
import pandas as pd
import psycopg2
from preprocessing import process_voltage_and_current


def cached_bison_data(filepath):
    df = pd.read_csv(filepath)
    print("Data columns: {}".format(df.columns))
    print("Number of rows: {}".format(len(df)))
    return df


def fetch_bison_data(filepath):
    device_ids = [
        57740,
        # 48034,
        33614,
        33404,
        33467,    
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
    # Alternatively, specify exact start and end times
    # start_time = datetime.now() - timedelta(days=30)
    # end_time = datetime.now() 
    start_time = '2024-09-01'#datetime.now() - timedelta(days=7)
    end_time = '2024-12-31'#datetime.now()
    # end_time = datetime.now() 

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
    # print("Generated SQL Query:")
    # print(query)
    # print("Query Parameters:")
    # print(params)

    # Set up your database connection (replace with your actual credentials)
    conn = psycopg2.connect(
        host=os.getenv('REDSHIFT_ENDPOINT'),
        port=os.getenv('REDSHIFT_PORT'),
        database=os.getenv('REDSHIFT_DBNAME'),
        user=os.environ.get('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASS')
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




def cached_site_info():
    datapath = "AllPumpCSV/"
    sites_info = [
    {
        'enable': False,
        'site_name':'Calumet SWD',
        'site_id':33614,
        'num_pumps': 1,
        'site_dir_name': 'CalumetSWD(33614)',
        'pump_curve_path': datapath+"PumpCurve_Calument_33614_DataPoints.csv", 
        'calibration_stages':
            [
                {
                    'frequency':    49,
                    'start_time':   '2024-12-13 12:00:00',
                    'end_time':     '2024-12-13 14:00:00',
                },
                {
                    'frequency':    51,
                    'start_time':   '2024-12-13 14:00:00',
                    'end_time':     '2024-12-13 16:00:00'
                },
                {
                    'frequency':    53,
                    'start_time':   '2024-12-13 16:00:00',
                    'end_time':     '2024-12-13 18:00:00'
                },
                {
                    'frequency':    55,
                    'start_time':   '2024-12-13 18:00:00',
                    'end_time':     '2024-12-13 20:00:00'
                },
            ]
        },
        {
        'enable': False,
        'site_name':'Canadian SWD',
        'site_id':57740,
        'num_pumps': 1,
        'site_dir_name': 'CanadianSWD(57740)',
        'pump_curve_path': datapath+"PumpCurve_Canadian_57740_DataPoints.csv",
        'calibration_stages':
            [
                {   
                    'frequency': 44,
                    'start_time': '2024-12-13 12:00:00',
                    'end_time':   '2024-12-13 14:00:00'
                },
                {
                    'frequency': 47,
                    'start_time': '2024-12-13 14:00:00',
                    'end_time':   '2024-12-13 16:00:00'
                },
                {
                    'frequency': 49,
                    'start_time': '2024-12-13 16:00:00',
                    'end_time':   '2024-12-13 18:00:00'
                },
                {
                    'frequency': 51,
                    'start_time': '2024-12-13 18:00:00',
                    'end_time':   '2024-12-13 20:00:00'
                },
                {
                    'frequency': 53,
                    'start_time': '2024-12-13 20:00:00',
                    'end_time':   '2024-12-13 22:00:00'
                },
                {
                    'frequency': 55,
                    'start_time': '2024-12-13 22:00:00',
                    'end_time':   '2024-12-14 00:00:00'
                },
                {
                    'frequency': 57,
                    'start_time': '2024-12-14 00:00:00',
                    'end_time':   '2024-12-14 02:00:00'
                },
                {
                    'frequency': 59,
                    'start_time': '2024-12-14 02:00:00',
                    'end_time':   '2024-12-14 04:00:00'
                },
            ]
        },
        {
        'enable': False,
        'site_name':'Siegrist SWD',
        'site_id':33467,
        'num_pumps': 1,
        'site_dir_name': 'SiegristSWD(33467)',
        'pump_curve_path': datapath+"PumpCurve_Siegrist_33467_DataPoints.csv",
        'calibration_stages':
            [
                {
                    'frequency':    47,
                    'start_time':   '2024-12-17 10:00:00',
                    'end_time':     '2024-12-17 12:00:00',
                },
                {
                    'frequency':    49,
                    'start_time':   '2024-12-17 14:00:00',
                    'end_time':     '2024-12-17 16:00:00'
                },
                {
                    'frequency':    51,
                    'start_time':   '2024-12-17 16:00:00',
                    'end_time':     '2024-12-17 18:00:00'
                },
                {
                    'frequency':    53,
                    'start_time':   '2024-12-17 18:00:00', 
                    'end_time':     '2024-12-17 20:00:00'
                },
                {
                    'frequency':    55,
                    'start_time':   '2024-12-17 20:00:00',
                    'end_time':     '2024-12-17 22:00:00'
                },
                {
                    'frequency':    57,
                    'start_time':   '2024-12-17 22:00:00',
                    'end_time':     '2024-12-18 00:00:00'
                },
            ]
        },
        {
        'enable': False,
        'site_name': 'Union City 2 SWD',
        'site_id': 33404,
        'num_pumps': 1,
        'site_dir_name': 'UnionCity2SWD(33404)',
        'pump_curve_path': datapath+"PumpCurve_UnionCity2_33404_DataPoints.csv",
        'calibration_stages':
            [
                {
                    'frequency':    46,
                    'start_time':   '2024-12-17 12:00:00',
                    'end_time':     '2024-12-17 14:00:00',
                },
                {
                    'frequency':    48,
                    'start_time':   '2024-12-17 14:00:00',
                    'end_time':     '2024-12-17 16:00:00',
                },
                {
                    'frequency':    50,
                    'start_time':   '2024-12-17 16:00:00',
                    'end_time':     '2024-12-17 18:00:00'
                },
                {
                    'frequency':    52,
                    'start_time':   '2024-12-17 18:00:00',
                    'end_time':     '2024-12-17 20:00:00'
                },
                {
                    'frequency':    54,
                    'start_time':   '2024-12-17 20:00:00',
                    'end_time':     '2024-12-17 22:00:00'
                },
                {
                    'frequency':    56,
                    'start_time':   '2024-12-17 22:00:00',
                    'end_time':     '2024-12-18 00:00:00'
                },
            ]
        },
    ]

    return sites_info
