drop schema if exists inventory_dwh cascade;

CREATE SCHEMA IF NOT EXISTS inventory_dwh;

drop table if exists inventory_dwh.customers;
create table inventory_dwh.customers(
	customer_id int primary key,
	city varchar(30),
	country varchar(30),
	age int,
	age_category varchar(30),
	yearly_income varchar(30),
	gender varchar(30) not null,
	total_children int,
	education varchar(30),
	occupation varchar(30)
);

drop table if exists inventory_dwh.products;
create table inventory_dwh.products(
  	product_id int primary key,
  	brand_name varchar(60) DEFAULT NULL,
  	product_name varchar(60) NOT NULL,
  	SKU bigint NOT null,
  	SRP decimal(10,4) DEFAULT NULL,
  	gross_weight real DEFAULT NULL,
  	net_weight real DEFAULT NULL,
  	recyclable_package smallint DEFAULT NULL,
  	low_fat smallint DEFAULT NULL,	
  	units_per_case smallint DEFAULT NULL,
  	cases_per_pallet smallint DEFAULT NULL,
  	shelf_width real DEFAULT NULL,
  	shelf_height real DEFAULT NULL,
  	shelf_depth real DEFAULT null,
  	product_subcategory varchar(30) default null,
  	product_category varchar(30) default null,
  	product_department varchar(30) default null,
  	product_family varchar(30) default null
  );

 
drop table if exists inventory_dwh.time_by_day;
create table inventory_dwh.time_by_day(
	time_id int primary key,
  	the_date timestamp DEFAULT NULL,
	the_day varchar(30) DEFAULT NULL,
	the_month varchar(30) DEFAULT NULL,
	the_year smallint DEFAULT NULL,
	day_of_month smallint DEFAULT NULL,
	week_of_year int DEFAULT NULL,
	month_of_year smallint DEFAULT NULL,
 	quarter varchar(30) DEFAULT NULL
);
 

drop table if exists inventory_dwh.warehouses;
create table inventory_dwh.warehouses(
	  warehouse_id int primary key,
	  stores_id int DEFAULT NULL,
	  warehouse_name varchar(60) DEFAULT NULL,
	  wa_address1 varchar(30) DEFAULT NULL,
	  wa_address2 varchar(30) DEFAULT NULL,
	  wa_address3 varchar(30) DEFAULT NULL,
	  wa_address4 varchar(30) DEFAULT NULL,
	  warehouse_city varchar(30)  DEFAULT NULL,
	  warehouse_state_province varchar(30) DEFAULT NULL,
	  warehouse_postal_code varchar(30) DEFAULT NULL,
	  warehouse_country varchar(30) DEFAULT NULL,
	  warehouse_owner_name varchar(30) DEFAULT NULL,
	  warehouse_phone varchar(30) DEFAULT NULL,
	  warehouse_fax varchar(30) DEFAULT null
  );
 
 
drop table if exists inventory_dwh.stores;
create table inventory_dwh.stores(
	  store_id int primary key,
	  store_type varchar(30) DEFAULT NULL,
	  store_name varchar(30) DEFAULT NULL,
	  store_number int DEFAULT NULL,
	  store_street_address varchar(30) DEFAULT NULL,
	  store_city varchar(30) DEFAULT NULL,
	  store_state varchar(30) DEFAULT NULL,
	  store_postal_code varchar(30) DEFAULT NULL,
	  store_country varchar(30) DEFAULT NULL,
	  store_manager varchar(30) DEFAULT NULL,
	  store_phone varchar(30) DEFAULT NULL,
	  store_fax varchar(30) DEFAULT NULL,
	  first_opened_date timestamp DEFAULT NULL,
	  last_remodel_date timestamp DEFAULT NULL,
	  store_sqft int DEFAULT NULL,
	  grocery_sqft int DEFAULT NULL,
	  frozen_sqft int DEFAULT NULL,
	  meat_sqft int DEFAULT NULL,
	  coffee_bar smallint DEFAULT NULL,
	  video_store smallint DEFAULT NULL,
	  salad_bar smallint DEFAULT NULL,
	  prepared_food smallint DEFAULT NULL,
	  florist smallint DEFAULT NULL
 );


drop table if exists inventory_dwh.promotions;
create table inventory_dwh.promotions(
	promotion_id int primary key,
  	promotion_name varchar(30) DEFAULT NULL,
 	media_type varchar(30) DEFAULT NULL,
  	cost decimal(10,4) DEFAULT NULL,
  	start_date timestamp DEFAULT NULL,
  	end_date timestamp DEFAULT NULL
);


drop table if exists inventory_dwh.inventory_fact;
create table inventory_dwh.inventory_fact(
	product_id int,
  	time_id int,
  	warehouse_id int,
  	store_id int,
	units_ordered int DEFAULT NULL,
	units_shipped int DEFAULT NULL,
 	warehouse_sales decimal(10,4) DEFAULT NULL,
 	warehouse_cost decimal(10,4) DEFAULT NULL,
 	supply_time smallint DEFAULT NULL,
 	store_invoice decimal(10,4) DEFAULT null,
 	promotion_id int,
 	primary key (product_id, time_id, warehouse_id, store_id),
 	foreign key (product_id) references inventory_dwh.products(product_id) ON DELETE set null,
 	foreign key (time_id) references inventory_dwh.time_by_day(time_id) ON DELETE set null,
 	foreign key (warehouse_id) references inventory_dwh.warehouses(warehouse_id) ON DELETE set null,
 	foreign key (store_id) references inventory_dwh.stores(store_id) ON DELETE set null,
 	foreign key (promotion_id) references inventory_dwh.promotions(promotion_id) ON DELETE set NULL
);

