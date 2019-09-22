# ##

import pandas as pd
import numpy as np


def items_available(items, item_label, item_dim, max_repeat):
    """
    items: items recurring list
    item_label: item label the function will build the groupby count
    item_dim: dimension dataframe with all items properties
    max_repeat: how many times each item dim needs to appear in target df
    """
    group_count_item = items.groupby(item_label).count()[[items.columns[1]]]
    group_count_item = group_count_item.rename(columns={group_count_item.columns[0]: 'count'})
    group_count_item = item_dim.merge(group_count_item, how='left', on=item_label)

    t = group_count_item[group_count_item['count'] != max_repeat][item_label].to_list()
    return t


# items_available(dim_analysts, 'supervisor_id', dim_supervisors, 5)

# %% Dim Supervisors


df_names = pd.read_excel('../pbi-4-business/fakedata/names.xlsx')

# df_supervisors['supervisor_id'] -> identity 1 to 10
supervisor_id = pd.Series(np.arange(1, 11))
supervisor_name = pd.Series(df_names.iloc[:10]['name'])

dim_supervisors = pd.DataFrame({'supervisor_id': supervisor_id.values
                                   , 'supervisor_name': supervisor_name.values})

# %% Dim Analysts

# create analyst_id and analyst name
analyst_id = pd.Series(np.arange(1, 51))
analyst_name = pd.Series(df_names.iloc[11:61]['name'])

dim_analysts = pd.DataFrame({'analyst_id': analyst_id.values,
                             'analyst_name': analyst_name.values})

dim_analysts['supervisor_id'] = dim_analysts['analyst_id'].map(lambda x: 0)

# iterate over analysts df and fill random supervisors
analyst_range = range(len(dim_analysts.index))
for analyst in analyst_range:
    items_available_list = items_available(dim_analysts, 'supervisor_id', dim_supervisors, 5)
    random_sup = np.random.choice(items_available_list)
    dim_analysts.at[analyst, 'supervisor_id'] = random_sup

# %% Dim Locations

municipios = pd.read_excel('..\\municipios.xlsx')
locations_sample = pd.DataFrame({'location_id': np.random.randint(len(municipios.index), size=6)})
dim_locations = locations_sample.merge(municipios,
                                       how='left',
                                       left_on='location_id',
                                       right_index=True)[['location_id', 'nome', 'latitude', 'longitude']]

# %% Dim Clients

# create analyst_id and analyst name
client_id = pd.Series(np.arange(1, 13))
client_name = pd.Series(df_names.iloc[62:74]['name'])

dim_clients = pd.DataFrame({'client_id': client_id.values,
                            'client_name': client_name.values})

dim_clients['location_id'] = dim_clients['client_id'].map(lambda x: 0)

# iterate over dim clients and fill location ramdomly
client_range = range(len(dim_clients.index))

for client in client_range:
    items_available_list = items_available(dim_clients, 'location_id', dim_locations, 2)
    random_location = np.random.choice(items_available_list)
    dim_clients.at[client, 'location_id'] = random_location

# %% Dim Category

category_id = pd.Series(np.arange(1, 5))
category_name = pd.Series(['Perifericos', 'Hardware', 'Software', 'Infra'])

dim_category = pd.DataFrame({'category_id': category_id,
                             'category_name': category_name})

# %%  Dim subcategory

sub_category_id = pd.Series(np.arange(1, 9))
sub_category_name = pd.Series(['Keyboard', 'Headset',
                               'Motherboard', 'RAM Memory',
                               'Visual Studio', 'Power BI',
                               'Chair', 'Desk'])
category_id = pd.Series([1, 1, 2, 2, 3, 3, 4, 4])

dim_sub_category = pd.DataFrame({'sub_category_id': sub_category_id,
                                 'sub_category_name': sub_category_name,
                                 'category_id': category_id})

dim_analysts.to_csv('dim_analyts.csv', index=False)
dim_supervisors.to_csv('dim_supervisors.csv', index=False)
dim_clients.to_csv('dim_clients.csv', index=False)
dim_locations.to_csv('dim_locations.csv', index=False)
dim_category.to_csv('dim_category.csv', index=False)
dim_sub_category.to_csv('dim_sub_category.csv', index=False)