import os
import pandas as pd
from mysql import connector
from pandas import DataFrame
from datetime import datetime

CUR_DIR = os.path.abspath(os.getcwd())

def age(birthday):
    birthday = datetime.strptime(birthday, "%Y-%m-%d")
    return 1999 - birthday.year - ((1, 1) < (birthday.month, birthday.day))

def age_category(age):
    if age in range (17,25):
        return "17 - 25"
    elif age in range (26, 35):
        return "26 - 35"
    elif age in range (36, 50):
        return "36 - 50"
    elif age in range (51, 64):
        return "51 - 64"
    elif age in range (65, 75):
        return "65 - 75"
    else:
        return ">75"

def transform_customer(data_df):
    data_df.dropna()
    data_df['age'] = data_df['birthdate'].apply(age)
    data_df['age_category'] = data_df['age'].apply(age_category)

    data_df = data_df.drop('birthdate', axis=1)

    #change position of age column
    temp_column = data_df.pop('age')
    data_df.insert(3, 'age', temp_column)
    #change position of age_category column
    temp_column = data_df.pop('age_category')
    data_df.insert(4, 'age_category', temp_column)

    file_name = "processed_customers.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    data_df.to_csv(file_path, index=False)

    return file_path


def transform_product(product_df: DataFrame, product_class_df: DataFrame):
    product_df.dropna()
    product_class_df.dropna()
    product_df = product_df.merge(product_class_df, how="inner", on=['product_class_id'])
    product_df = product_df.drop(columns=['product_class_id'])

    file_name = "processed_products.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    product_df.to_csv(file_path, index=False)

    return file_path

def transform_promotion(promotion_df: DataFrame):
    options = [110, 0] 
    promotion_df = promotion_df[promotion_df['promotion_district_id'].isin(options)]
    promotion_df = promotion_df.drop(columns=['promotion_district_id'])

    file_name = "processed_promotions.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    promotion_df.to_csv(file_path, index=False)

    return file_path

def transform_store(store_df: DataFrame):
    store_df = store_df.drop(columns=['region_id'])

    file_name = "processed_stores.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    store_df.to_csv(file_path, index=False)

    return file_path

def transform_warehouse(warehouse_df: DataFrame):
    warehouse_df = warehouse_df.drop(columns=['warehouse_class_id'])
    
    file_name = "processed_warehouses.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    warehouse_df.to_csv(file_path, index=False)

    return file_path

def transform_time(time_df: DataFrame):
    file_name = "processed_times.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    time_df.to_csv(file_path, index=False)

    return file_path

def process_inventory(inventory_df: DataFrame, time_df: DataFrame, promotion_df: DataFrame):
    #data pre-processing
    inventory_df = inventory_df[inventory_df['is_deleted'] == 0]
    inventory_df = inventory_df.drop(columns=['is_deleted'])
    #join 2 table inventory and time_by_day
    inventory_time_df = inventory_df.merge(time_df, how="inner", on=['time_id'])

    #find promotion_id match time of inventory fact
    promotion_id_list = []
    for i_idx, i_row in inventory_time_df.iterrows():
        inventory_time = datetime.strptime(i_row['the_date'], "%Y-%m-%d")
        found = False
        #print (type(promotion_df['start_date'][0]))
        #print (promotion_df['start_date'][1])
        for idx, row in promotion_df.iterrows():
            #check since the first record startdate is nan which is float
            if isinstance(row['start_date'], float): continue

            start_time = datetime.strptime(row['start_date'], "%Y-%m-%d")
            end_time = datetime.strptime(row['end_date'], "%Y-%m-%d")
            if (inventory_time >= start_time) and (inventory_time <= end_time):
                found = True
                promotion_id_list.append(row['promotion_id'])
                break
        if not found: 
            promotion_id_list.append(0)
    
    
    #promotion_id_dict = {'promotion_id': promotion_id_list}
    inventory_df['promotion_id'] = promotion_id_list
    return inventory_df

def transform_concat_inventory(paths_to_csv: str, path_to_promotion: str):
    inv97_df = pd.read_csv(paths_to_csv['inventory_fact_1997'])
    inv98_df = pd.read_csv(paths_to_csv['inventory_fact_1998'])
    time_df = pd.read_csv(paths_to_csv['time_by_day'])
    proc_promotion_df = pd.read_csv(path_to_promotion)

    df1 = process_inventory(inv97_df, time_df, proc_promotion_df)
    result_df = pd.concat([df1, process_inventory(inv98_df, time_df, proc_promotion_df)], axis=0)

    file_name = "processed_invetory.csv"
    file_path = CUR_DIR + '/temp_data/' + file_name
    result_df.to_csv(file_path, index=False)

    return file_path