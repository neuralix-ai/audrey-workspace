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
    closest_timeidx = date_subset.iloc[0].index
    for i, row in date_subset.iterrows():
        curr_date = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
        if abs(query_datetime-curr_date) < abs(query_datetime-closest_timestamp):
            closest_timestamp = curr_date
            closest_timeidx = i
    return closest_timestamp, closest_timeidx # errs on the side of being later