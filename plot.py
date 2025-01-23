import matplotlib.pyplot as plt
import numpy as np

def plot_ts_gt(sitegts, freq_data, site_id):
    # plot the time series estimated ground truth
    # the first plot is the time series and estiamted ground truth
    # the second plot is a set of surrounding indices before, during, and after the estimated ground truth for visual clarity
    # Note: Audrey will switch to plotly.express in the future
    # ------------------------------------------------
    # sitegts: a dictionary of dictionaries, see format_sitegts()
    # freq_data: the frequency information of the dataframe/series from the dataframe
    # site_id: (int)
    # output: None
    # ================================================
    
    # this is as many different colors you can assign to frequencies in calibration; add more as needed
    colors = ['c','m','y','r','g','b','lime','violet']
    site_gt = sitegts[site_id]
    fig = plt.figure(figsize=(30,4))
    plt.plot(np.arange(len(freq_data)), freq_data,label="Frequency TS")
    for i, freq in enumerate(site_gt.keys()):
        curr_start = site_gt[freq][0]
        curr_end = site_gt[freq][1]
        plt.plot(np.arange(curr_start,curr_end), freq_data[curr_start:curr_end], c=colors[i])
    plt.grid()
    plt.show()

    # Zoom In
    freqs = list(site_gt.keys())
    first_freq = freqs[0]
    last_freq = freqs[-1]
    fig = plt.figure(figsize=(20,4))
    range_start = site_gt[first_freq][0]-200
    range_end = site_gt[last_freq][1]+200
    plt.plot(np.arange(range_start,range_end), freq_data[range_start:range_end], label="Frequency TS")
    for i, freq in enumerate(site_gt.keys()):
        curr_start = site_gt[freq][0]
        curr_end = site_gt[freq][1]
        plt.plot(np.arange(curr_start,curr_end), freq_data[curr_start:curr_end], c=colors[i])
    plt.grid()
    plt.show()
    return