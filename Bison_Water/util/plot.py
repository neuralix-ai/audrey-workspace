import matplotlib.pyplot as plt
from copy import deepcopy
import numpy as np
import plotly.express as px


def map_frequency_to_color(frequencies): # from Ryan -- with a few alterations
    # Map to color index
    # Define the colorscale
    colorscale = px.colors.sequential.Turbo
    n_colors = len(colorscale)
    color_index = lambda freq: int((freq - min(freq)) / (max(freq) - min(freq)) * (n_colors - 1))
    color_map = color_index(frequencies)
    return color_map


def plot_3kpis(site_data, site_name):
    # plots the three kpis (kWh/BBL, Flow Rate, and Percent from BEP in a 3D scatter plot) -- can adapt this to accept any three kpis
    # ------------------------------------------------
    # site_data: a dictionary of dictionaries, see dataloader.py
    # site_name: (str) the name of the site
    # site_id: (int)
    # output: None
    # ================================================
    fig = px.scatter_3d(site_data,x='flow rate', y='kWh/BBL',z='perc_from_BEP', 
                        color="frequency",opacity=0.8,color_continuous_scale=px.colors.sequential.Turbo,
                        title="Site: {}".format(site_name),height=700,width=800)
    # Update the marker size
    fig.update_traces(marker=dict(size=3))
    # Create the meshgrid for the plane
    x_min, x_max = min(site_data['flow rate']), max(site_data['flow rate'])
    y_min, y_max = site_data['kWh/BBL'].min(), site_data['kWh/BBL'].max()
    x = np.arange(x_min, x_max, 10)
    y = np.arange(y_min, y_max, 0.1)
    X, Y = np.meshgrid(x, y)
    Z = np.zeros_like(X)
    fig.add_surface(x=X, y=Y, z=Z, opacity=1) # Add the plane as a surface
    plt.show()
    return


def plot_ts_gt(sitegts, freq_data_, site_id, site_bounds): # call this on a per site basis
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
    freq_data = deepcopy(freq_data_)
    freq_data.replace(0, np.nan, inplace=True)
    colors = ['c','m','y','r','g','b','lime','violet']
    site_gt = sitegts[site_id]
    fig = plt.figure(figsize=(30,4))
    plt.plot(np.arange(site_bounds[0],site_bounds[1]), freq_data[site_bounds[0]:site_bounds[1]],label="Frequency TS")
    for i, freq in enumerate(site_gt.keys()):
        curr_start = site_gt[freq][0]
        curr_end = site_gt[freq][1]
        plt.plot(np.arange(curr_start,curr_end), freq_data[curr_start:curr_end], c=colors[i], label="{} Hz".format(freq))
    plt.legend()
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
        plt.scatter(np.arange(curr_start,curr_end), freq_data[curr_start:curr_end], c=colors[i], label="{} Hz".format(freq))
    plt.legend()
    plt.grid()
    plt.show()
    return