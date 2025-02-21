from util.calculations import *
import numpy as np
import pandas as pd

def compute_kpis_for_sites(site_data, site_id, sites_info=None, site_pump_curves=None, kpis=['kWh/BBL','Flow Rate']):
    # computes the list of KPIs for each site
    # ------------------------------------------------
    # site_data: the dataframe of information for a given site and pump
    # site_id: (int) the site_id
    # kpis: the list of kpis to compute -- other supported kpis are 'perc_from_BEP' and 'norm_perc_from_BEP'

    # optional parameters: -- relevant if computing 'perc_from_BEP' or 'norm_perc_from_BEP'
    # site_pump_curves: these are the actual pump curves, which is only required if you're normalizing pump curves; see demo usage in pumpcurve_kpis.ipynb
    # sites_info: this is needed to know the data from the calibration stages
    # NOTE: if you are computing the normalized percent from BEP (norm_perc_from_BEP) then you must also calculate percent from BEP (perc_from_BEP)

    # output: (int) the average time in minutes between samples
    # ================================================

    site_data['frequency'] = site_data['frequency'].round() # round freqs to nearest whole
    site_freqs = np.unique(site_data['frequency']) # find unique freqs
    if 'Flow Rate' in kpis:
        site_data['flow rate'] = site_data['flow rate'].round() # round flow rate to nearest whole
    if 'kWh/BBL' in kpis:
        site_data = calc_kWh_BBL(site_data)
    if 'perc_from_BEP' in kpis:
        site_data = calc_perc_BEP(site_data,site_pump_curves)
        site_data['abs_calc_perc_BEP'] = abs(site_data['perc_from_BEP'])
    if 'norm_perc_from_BEP' in kpis:
        calibration_stages = sites_info[site_id]['calibration_stages']
        site_data, unnormalized_freqs, interpolated_freqs, normalized_freqs = normalize_BEP(site_data, calibration_stages)
        site_data['abs_norm_calc_perc_BEP'] = abs(site_data['norm_perc_from_BEP'])
        print("Recorded frequencies outside calibration range (KPI is not normalized): {}".format(unnormalized_freqs))
        print("Interpolated frequencies within calibration range (KPI normalization is approximated): {}".format(interpolated_freqs))
        print("Normalized frequencies: {}".format(normalized_freqs))

    # format it into a chart, averaging values per frequnecy
    sampling_interval = estimate_sampling_interval(site_data['timestamp_datetime'])
    site_pump_kpis = {}
    for site_freq in site_freqs: # for each of these freqs...
        site_freq_data = site_data[site_data['frequency']==site_freq]
        if len(site_freq_data) >= 60/sampling_interval: # one hour
            flow_data = round(site_freq_data['flow rate'].mean(),3) # avg flow rate @ this freq
            freq_kpis = {} # Bison KPIs
            if 'Flow Rate' in kpis:
                freq_kpis['Flow Rate'] = flow_data
            if 'kWh/BBL' in kpis:
                freq_kpis['kWh/BBL'] = round(site_freq_data['kWh/BBL'].mean(),3) # Calculate avg KWh/BBL @ this freq per site
            if 'perc_from_BEP' in kpis:
                freq_kpis['perc_from_BEP'] = round(site_freq_data['perc_from_BEP'].mean(),3)# Calculate % BEP @ this freq
                freq_kpis['abs_perc_from_BEP'] = abs(freq_kpis['perc_from_BEP'])
            if 'norm_perc_from_BEP' in kpis:
                freq_kpis['norm_perc_from_BEP'] = round(site_freq_data['norm_perc_from_BEP'].mean(),3)
                freq_kpis['abs_norm_perc_from_BEP'] = abs(freq_kpis['norm_perc_from_BEP'])
            site_pump_kpis[site_freq] = freq_kpis
    return site_data, site_pump_kpis, sampling_interval


def kpi_charts(kpis_all_sites, site_ids, sites_info, kpis=['kWh/BBL','Flow Rate'],print_chart=False):
    # generates the Optimize-For-KPI Charts for each of the given sites
    # ------------------------------------------------
    # kpis_all_sites: a dictionary of dictionaries, see average_kpis_by_freq.ipynb
    # site_ids: list of (int) site_ids
    # output: df_all is a dataframe with all the sites' KPI information averaged on a per-frequency basis
            # game_kpi reports the best frequency to use, if you were to gamify/optimize for that particular KPI
    # NOTE: a single sites' KPI chart is saved to a CSV
    # ================================================

    df_all = pd.DataFrame(columns=['site_id','kWh/BBL','Flow Rate'])
    for site_id in site_ids:
        site_name = sites_info[site_id]['site_name']
        print(site_name)
        
        site_kpis = kpis_all_sites[site_id]
        df_site = pd.DataFrame(site_kpis).transpose()
        df_site['site_id'] = site_name
        if print_chart:
            display(df_site)

        df_all = pd.concat([df_all,df_site])

        game_kpi = {'Flow Rate': int(df_site[df_site['Flow Rate']==max(df_site['Flow Rate'])].index[0]),
                    'kWh/BBL': int(df_site[df_site['kWh/BBL']==min(df_site['kWh/BBL'])].index[0])}
        if 'perc_from_BEP' in kpis:
            game_kpi['perc_from_BEP'] = int(df_site[df_site['abs_perc_from_BEP']==min(df_site['abs_perc_from_BEP'])].index[0])
        if 'norm_perc_from_BEP' in kpis:
            game_kpi['norm_perc_from_BEP'] = int(df_site[df_site['abs_norm_perc_from_BEP']==min(df_site['abs_norm_perc_from_BEP'])].index[0])

        df_site.to_csv("bison_kpi_charts/{}_kpis_unnormalized.csv".format(site_id))
    return df_all, game_kpi
