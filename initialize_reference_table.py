from dags.helpers.connections import Postgresql
import pandas as pd
from datetime import datetime
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
database = Postgresql(host='localhost', port='5432', db_name='postgres', user_name='postgres', password='')

# initialize reference table
DB_NAME = 'inventory_etl_manager'
#database.drop_schema(DB_NAME)
database.create_schema(DB_NAME)
database.create_table(table_schema=DB_NAME, table_name='database_flow_reference_table',
                      columns={'insert_date': 'timestamp',
                               'source_connection': 'varchar',
                               'source_schema': 'varchar',
                               'source_table': 'varchar',
                               'key_fields': 'varchar',
                               'extraction_method': 'varchar',
                               'extraction_type': 'varchar',
                               'destination_connection': 'varchar',
                               'destination_schema': 'varchar',
                               'destination_table': 'varchar',
                               'target_fields': 'varchar'})

database.truncate_table(table_schema=DB_NAME, table_name='database_flow_reference_table')

list_of_dict = []

customers_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'customer', 
                       'key_fields': 'customer_id, city, country, birthdate, yearly_income, gender, total_children, education, occupation',

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'customers', 
                       'target_fields': 'customer_id, city, country, age, age_category, yearly_income, gender, total_children, education, occupation'}
list_of_dict.append(customers_dict)

promotions_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'promotion', 
                       'key_fields': 'promotion_id, promotion_district_id, promotion_name, media_type, cost, start_date, end_date', 

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'promotions', 
                       'target_fields': 'promotion_id, promotion_name, media_type, cost, start_date, end_date'}
list_of_dict.append(promotions_dict)


products_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'product', 
                       'key_fields': 'product_id, product_class_id, brand_name, product_name, sku, srp, gross_weight, net_weight, recyclable_package, low_fat, units_per_case, cases_per_pallet, shelf_width, shelf_height, shelf_depth', 

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'products', 
                       'target_fields': 'product_id, brand_name, product_name, sku, srp, gross_weight, net_weight, recyclable_package, low_fat, units_per_case, cases_per_pallet, shelf_width, shelf_height, shelf_depth, product_subcategory, product_category, product_department, product_family'}
list_of_dict.append(products_dict)

products_class_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'product_class', 
                       'key_fields': 'product_class_id, product_subcategory, product_category, product_department, product_family', 

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': '', 
                       'target_fields': ''}
list_of_dict.append(products_class_dict)


stores_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'store', 
                       'key_fields': 'store_id, store_type, region_id, store_name, store_number, store_street_address, store_city, store_state, store_postal_code, store_country, store_manager, store_phone, store_fax, first_opened_date, last_remodel_date, store_sqft,grocery_sqft, frozen_sqft, meat_sqft, coffee_bar, video_store, salad_bar, prepared_food,florist',

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'stores', 
                       'target_fields': 'store_id, store_type, store_name, store_number, store_street_address, store_city, store_state, store_postal_code, store_country, store_manager, store_phone, store_fax, first_opened_date, last_remodel_date, store_sqft,grocery_sqft, frozen_sqft, meat_sqft, coffee_bar, video_store, salad_bar, prepared_food,florist'}
list_of_dict.append(stores_dict)


warehouses_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'warehouse', 
                       'key_fields': 'warehouse_id, warehouse_class_id, stores_id, warehouse_name, wa_address1, wa_address2, wa_address3, wa_address4, warehouse_city, warehouse_state_province, warehouse_postal_code, warehouse_country, warehouse_owner_name, warehouse_phone, warehouse_fax',

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'warehouses', 
                       'target_fields': 'warehouse_id, stores_id, warehouse_name, wa_address1, wa_address2, wa_address3, wa_address4, warehouse_city, warehouse_state_province, warehouse_postal_code, warehouse_country, warehouse_owner_name, warehouse_phone, warehouse_fax'}
list_of_dict.append(warehouses_dict)


time_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'time_by_day', 
                       'key_fields': 'time_id, the_date, the_day, the_month, the_year, day_of_month, week_of_year, month_of_year, quarter',

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'time_by_day', 
                       'target_fields': 'time_id, the_date, the_day, the_month, the_year, day_of_month, week_of_year, month_of_year, quarter'}
list_of_dict.append(time_dict)


inventory97_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'inventory_fact_1997', 
                       'key_fields': 'product_id, time_id, warehouse_id, store_id, units_ordered, units_shipped, warehouse_sales, warehouse_cost, supply_time, store_invoice, is_deleted',

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': 'inventory_fact', 
                       'target_fields': 'product_id, time_id, warehouse_id, store_id, units_ordered, units_shipped, warehouse_sales, warehouse_cost, supply_time, store_invoice, promotion_id'}
list_of_dict.append(inventory97_dict)


inventory98_dict = {'insert_date': str(datetime.now()), 
                       'source_connection': 'mysql',
                       'source_schema': 'mmis',
                       'source_table': 'inventory_fact_1998', 
                       'key_fields': 'product_id, time_id, warehouse_id, store_id, units_ordered, units_shipped, warehouse_sales, warehouse_cost, supply_time, store_invoice, is_deleted',

                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 

                       'destination_connection': 'postgresql',
                       'destination_schema': 'inventory_dwh',
                       'destination_table': '', 
                       'target_fields': 'product_id, time_id, warehouse_id, store_id, units_ordered, units_shipped, warehouse_sales, warehouse_cost, supply_time, store_invoice, promotion_id'}
list_of_dict.append(inventory98_dict)

for dict in list_of_dict:
    dict = {k: [v, ] for k, v in dict.items()}
    temp_df = pd.DataFrame(dict)

    database.insert_values(data=temp_df, table_schema=DB_NAME, table_name='database_flow_reference_table',
                        columns=', '.join(temp_df.columns.tolist()))

database.close_connection()