# ##

import pandas as pd
import numpy as np
from decimal import *
from calendar import monthrange
import datetime

getcontext().prec = 7


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


if __name__ == '__main__':

    # %% Dim Supervisors
    folder_path = 'C:\\Users\\a116822\\dev\\fake-data-python\\'
    df_names = pd.read_excel('{folder}names.xlsx'.format(folder=folder_path))

    supervisor_id = pd.Series(np.arange(1, 4))
    supervisor_name = pd.Series(df_names.iloc[:3]['name'])

    dim_supervisors = pd.DataFrame({'supervisor_id': supervisor_id.values, 'supervisor_name': supervisor_name.values})

    # %% Dim Analysts

    analyst_id = pd.Series(np.arange(1, 16))
    analyst_name = pd.Series(df_names.iloc[5:20]['name'])

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

    municipios = pd.read_excel('{folder}municipios.xlsx'.format(folder=folder_path))
    locations_sample = pd.DataFrame({'location_id': np.random.randint(len(municipios.index), size=10)})
    dim_locations = locations_sample.merge(municipios,
                                           how='left',
                                           left_on='location_id',
                                           right_index=True)[['location_id', 'nome', 'latitude', 'longitude']]

    # %% Dim Clients

    client_id = pd.Series(np.arange(1, 51))
    client_name = pd.Series(df_names.iloc[21:71]['name']).reset_index(drop=True)

    dim_clients = pd.DataFrame({'client_id': client_id.values,
                                'client_name': client_name.values})

    dim_clients['location_id'] = dim_clients['client_id'].map(lambda x: 0)

    # iterate over dim clients and fill location ramdomly
    client_range = range(len(dim_clients.index))

    for client in client_range:
        items_available_list = items_available(dim_clients, 'location_id', dim_locations, 5)
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

    # %% fact requests
    
    def get_rows_per_month(date):
        if date.month < 7:
            rand = np.random.randint(3500, 4050)
        else:
            rand = np.random.randint(4100, 5000)
        return rand
    
    def build_month_year_date(year, month):
        return datetime.date(year, month, 1)
    
    years_list = [2018, 2019]
    months = pd.Series([build_month_year_date(x, y) for x in years_list for y in np.arange(1, 13)])
    rows = pd.Series([get_rows_per_month(x) for x in months])
    months = pd.Series([str(x.year)+'-'+str(x.month) +'-'+ '1' for x in months])
    
    df_rows_per_month_year = pd.DataFrame({'month_year': months, 'rows': rows.values})
    
    pd.DataFrame[{'rows_quantity': rows}]
       
    requests_quantity = []
    for i in range(len(months)):
        if i < 7:
            count_rows = np.random.randint(3500, 4050)
        else:
            count_rows = np.random.randint(4100, 5000)

        requests_quantity.append(count_rows)

    month_size = pd.DataFrame({'month': months, 'requests_quantity': requests_quantity})
    month_size['year'] = month_size['month'].map(lambda x: years)
    # month = 1
    # requests_quantity = 3995

    fact_requests = pd.DataFrame()
    for month, requests_quantity in month_size.itertuples(index=False):

        t = pd.DataFrame()
        t['request_id'] = np.arange(requests_quantity)

        # sub-categoria randomization
        sub_category_id_list = dim_sub_category['sub_category_id'].to_list()
        dec_low = Decimal(.2) / 2
        dec_medium = Decimal(.6) / 2
        dec_high = Decimal(.2) / 4
        sub_category_id_p = [dec_low, dec_low, dec_medium, dec_medium, dec_high, dec_high, dec_high, dec_high]
        t['sub_category_id'] = t['request_id'].\
            map(lambda x: np.random.choice(sub_category_id_list, p=sub_category_id_p))

        # clients randomization
        localization_id_list = dim_locations['location_id'].to_list()

        dec_low = Decimal(.2) / 4
        dec_medium = Decimal(.6) / 2
        dec_high = Decimal(.2) / 4
        localization_p_list = [dec_low, dec_low, dec_low, dec_low, dec_medium,
                               dec_medium, dec_high, dec_high, dec_high, dec_high]
        dim_locations['valor_p'] = localization_p_list
        dim_clients['valor_p_location'] = dim_clients.merge(dim_locations,
                                                            how='left',
                                                            on='location_id')['valor_p'].map(lambda x: x / 5)

        # set clients_id_list
        client_id_list = dim_clients['client_id'].to_list()
        client_p_list = dim_clients['valor_p_location'].to_list()
        t['client_id'] = t['request_id'].map(lambda x: np.random.choice(client_id_list, p=client_p_list))

        # set analysts id lists
        dec_low = Decimal(.2) / 6
        dec_medium = Decimal(.2) / 7
        dec_high = Decimal(.6) / 2
        analyst_id_list = dim_analysts['analyst_id'].to_list()
        analyst_p_list = [dec_low, dec_low, dec_low, dec_low, dec_low, dec_low,
                          dec_medium, dec_medium, dec_medium, dec_medium,
                          dec_medium, dec_medium, dec_medium, dec_high, dec_high]
        t['analyst_id'] = t['request_id'].map(lambda x: np.random.choice(analyst_id_list, p=analyst_p_list))
        t['mes_criacao'] = t['request_id'].map(lambda x: month)
        fact_requests = pd.concat([fact_requests, t])
    
    folder_path = folder_path + 'result\\'
    fact_requests.to_csv('{folder}fact_requests.csv'.format(folder=folder_path), index=False)
    dim_analysts.to_csv('{folder}dim_analysts.csv'.format(folder=folder_path), index=False)
    dim_supervisors.to_csv('{folder}dim_supervisors.csv'.format(folder=folder_path), index=False)
    dim_clients.to_csv('{folder}dim_clients.csv'.format(folder=folder_path), index=False)
    dim_locations.to_csv('{folder}dim_locations.csv'.format(folder=folder_path).format(folder=folder_path),
                         index=False, encoding='utf-8-sig')
    dim_category.to_csv('{folder}dim_category.csv'.format(folder=folder_path), index=False)
    dim_sub_category.to_csv('{folder}dim_sub_category.csv'.format(folder=folder_path), index=False)
