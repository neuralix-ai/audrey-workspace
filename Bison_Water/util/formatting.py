from copy import deepcopy
import pandas as pd
import numpy as np
from util.preprocessing import find_closest_time


def format_sitegts(site_start,site_freqs,duration_len):
    # Create approximate labels for dataframe indices when each frequency is being calibrated
    # This is based on our two-hour calibration time for each frequency
    # ------------------------------------------------
    # site_start: (int) index in the all-site-data dataframe labelling when calibration begins
    # site_freqs: (list <int>) a list of frequencies being calibrated
        # assumptions:
            # frequencies are each calibrated for the same amount of time
            # frequencies are listed in the order they are calibrated (e.g. if calib. is done in ascending order, freqs. are listed ascending, too)
    # duration_len: (int) duration (the number of indices/timestamps) the calibration is occuring
        # assumption: the dataframe with site info is recorded in order; i.e. all the rows are in chronological order

    # output: a dictionary where key:value is frequency: [start_idx, end_idx] for that frequency during calibration
    # ================================================
    sitegt = {freq:[0,0] for freq in site_freqs}
    for freq in site_freqs:
        sitegt[freq][0] = site_start
        sitegt[freq][1] = site_start+duration_len
        site_start = site_start+duration_len+1
    return sitegt


# we can rename these functions later

def ryan_format(data, ryan_sites_info, audrey_sitegts):
    # takes Audrey's info in Audrey dictionaries-using-dataframe-indices format and converts it to Ryan's timestamp format
    # ------------------------------------------------
    # data: dataframe of all site info
    # ryan_sites_info: copied from https://github.com/neuralix-ai/dev_RyanMercer/blob/dev/notebooks/Customers/Bison/2024-12-29_Bison_PumpCurve_x-axisHealth_calibrated_alert.ipynb
    # audrey_sitegts: audrey's format of using dataframe indices
    # output: start-time and end-time and frequency data is recorded in Ryan's site_info format consistent with ryan_sites_info
    # ================================================

    ryan_format_sitegts = deepcopy(ryan_sites_info) # we need to keep the rest of the structure of Ryan's sites_info (enable, num_pumps, etc.)
    for site in ryan_format_sitegts: # for each site in the database...
        format_calib_stage = []
        site_id = site['site_id'] # for each site in Ryan's gt,
        curr_site_gts = audrey_sitegts[site_id] # find the site estimated gt in Audrey's idx format dictionary
        for freq in curr_site_gts.keys(): # for each frequency at a site...
            tmp = {}
            tmp['frequency'] = freq
            start_and_end = curr_site_gts[freq]
            start_timestamp = data.iloc[start_and_end[0]]['timestamp']
            end_timestamp = data.iloc[start_and_end[1]]['timestamp']
            tmp['start_time'] = start_timestamp
            tmp['end_time'] = end_timestamp
            format_calib_stage.append(tmp)
        site['calibration_stages'] = format_calib_stage # ...now replace the info now that it's in Ryan's format
    return ryan_format_sitegts


def ryan_site_info_to_audrey_format(data, ryan_cached_gt,sampling_rates):
    # takes in the GT format from Ryan and converts it to the format Audrey uses in the plots above
    # ------------------------------------------------
    # data: dataframe of all site info
    # ryan_cached_gt: copied from https://github.com/neuralix-ai/dev_RyanMercer/blob/dev/notebooks/Customers/Bison/2024-12-29_Bison_PumpCurve_x-axisHealth_calibrated_alert.ipynb
    # sampling_rates: sampling rates for each site (hardcoded)
    # output: audrey-format dictionaries about site info
    # ================================================
    audrey_format = {}
    for site in ryan_cached_gt:
        site_id = site['site_id']
        frequencies = []
        for stage in site['calibration_stages']:
            frequencies.append(stage['frequency'])
            listed_start = stage['start_time'].split(" ")
            startdate = listed_start[0]
            starttime = listed_start[1]
            _, index = find_closest_time(data[data['site_id']==site_id], startdate, query_time=starttime)
        site_estimatedgt = format_sitegts(index,frequencies,sampling_rates[site_id])
        audrey_format[site_id] = site_estimatedgt
    return audrey_format