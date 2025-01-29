import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt

column_names = ['unique_key','created_date','closed_date','agency','agency_name','complaint_type','descriptor','location_type',
                'incident_zip','incident_address','street_name','cross_street_1','cross_street_2','intersection_street_1',
                'intersection_street_2','address_type','city','landmark','facility_type','status','due_date','resolution_description',
                'resolution_action_updated_date','community_board','bbl','borough','x_coordinate_state_plane','y_coordinate_state_plane',
                'open_data_channel_type','park_facility_name','park_borough','vehicle_type','taxi_company_borough','taxi_pick_up_location',
                'bridge_highway_name','bridge_highway_direction','road_ramp','bridge_highway_segment','latitude','longitude','location']

def load_data(usecols):
    column_names_use = [column_names[ind] for ind in usecols]
    dtypes = {k: 'object' for k in column_names_use}

    df = dd.read_csv('nyc_311_2020.csv', header=None, names=column_names_use, dtype=dtypes, usecols=usecols)
    
    return df

def convert_datetime(df, colname):
    date_format = '%m/%d/%Y %I:%M:%S %p'
    df[colname] = dd.to_datetime(df[colname], format=date_format, errors='coerce')
    
    return df

def filter_data_by_date(df, datecol, start_date, end_date):
    start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
    end_date = pd.to_datetime(end_date, format="%Y-%m-%d")
    
    df_filtered = df[(df[datecol] >= start_date) & (df[datecol] <= end_date)]
    
    return df_filtered

data = load_data([1, 5, 6])
all_complaint_types = data['complaint_type'].value_counts().compute().to_dict()
print(all_complaint_types)

data_noise = data[data['complaint_type'].str.contains('Noise')]
all_complaint_causes = data_noise['descriptor'].value_counts().compute().to_dict()
print(all_complaint_causes)

data_noise = convert_datetime(data_noise, 'created_date')

quarters = [('2020-01-01', '2020-03-31'), ('2020-04-01', '2020-06-30'), 
            ('2020-07-01', '2020-09-30'), ('2020-10-01', '2020-12-31')]

quarters_with_causes = {}
quarters_with_num_complaints = {}

for (start_date, end_date) in quarters:
    data_noise_filtered = filter_data_by_date(data_noise, 'created_date', start_date, end_date)
    all_complaint_causes = data_noise_filtered['descriptor'].value_counts().compute().to_dict()
    
    quarters_with_causes[(start_date, end_date)] = all_complaint_causes
    quarters_with_num_complaints[(start_date, end_date)] = len(data_noise_filtered)
    print(all_complaint_causes)

for (start_date, end_date) in quarters:
    all_complaint_causes = quarters_with_causes[(start_date, end_date)]
    
    filtered_complaints_causes = {}
    for idx, k in enumerate(all_complaint_causes):
        if idx == 5: break
        filtered_complaints_causes[k] = all_complaint_causes[k]
    
    print(filtered_complaints_causes)

fig, ax = plt.subplots(nrows=2, ncols=2, figsize=(12, 12))

for qind, (start_date, end_date) in enumerate(quarters):
    num_complaints = quarters_with_num_complaints[(start_date, end_date)]
    all_complaint_causes = quarters_with_causes[(start_date, end_date)]
    
    filtered_complaints_causes = {}
    for idx, k in enumerate(all_complaint_causes):
        if idx == 5: break
        filtered_complaints_causes[k] = (all_complaint_causes[k]/num_complaints)*100
    
    ax[qind//2, qind%2].bar(range(len(filtered_complaints_causes)), list(filtered_complaints_causes.values()), align='center')
    ax[qind//2, qind%2].set_xticks(range(len(filtered_complaints_causes)), list(filtered_complaints_causes.keys()))
    
    ax[qind//2, qind%2].set_title('%s - %s' % (start_date, end_date))
    ax[qind//2, qind%2].set_ylabel('Percentage of Noise Complaints (%)')
    ax[qind//2, qind%2].set_ylim(0, 100)
    
    plt.setp(ax[qind//2, qind%2].get_xticklabels(), rotation=30, horizontalalignment='right')

plt.tight_layout()
plt.show()
