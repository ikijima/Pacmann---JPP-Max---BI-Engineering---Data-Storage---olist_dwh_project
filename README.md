# Pacmann---JPP-Max---BI-Engineering---Data-Storage---olist_dwh_project

*Test runs and answers can be found in the Pacmann jupyter notebook inside this repo*

This project is an attempt to design a data warehouse and data pipeline for an e-commerce platform. Olist, the e-commerce in question, is a Brazilian venture, connecting various sellers and customers across the nation. Therefore, the need for a robust data warehouse and data pipeline design is of utmost importance. Here, we are trying to provide the solution for those two concerns.

#### **Pretend Interview with Shareholders**

##### 1. Which dimension tables are likely to have changes overtime that we should track of?
  
  **Response**
  
  Primarily `customers` and `sellers`. </br>
  Customers may change their address or city (captured via `geolocation`) and sellers sometimes update their location or business names. </br>
  `products` and `category name` are mostly static, but we may occassionally reclassify a product category
  
##### 2. Do we need to maintain historical versions of data?
  
  **Response**
  
  We wanted to preserve historical changes for both `customers` and `sellers`, especially location changes, as they can affect delivery time and logistics. </br>
  For `products` and `category_name`, just keeping the latest version is enough since changes are rare and typically administrative </br>
  
##### 3. What kind of historical tracking is preferred, -- full versioning (type 2), overwrite (type 1), or current vs previous field (type 3)?
  
  **Response**
  
  SCD Type 2 (full-versioning)
  
  - `dim_customers`
  - `dim_sellers`
  
  SCD Type 1 (overwrite)
  
  - `dim_category_name`
  - `dim_products`

##### 4. How should we handle geolocation changes? Should customers and sellers be linked to geolocation with historical traceability?
  
  **Response**
  
  Yes, because geolocation information, current or previous, is valuable for analysis, eg. comparing delivery times or satisfaction scores before and after a move. </br>
  So we'll need foreign key-based Type 2 handling for geolocation information.
  
##### 5. Who will consume the historical data, and how will it be used (eg. in reports, machine learning, fraud detection, etc)?
  
  **Response**
  
  - Data analyst and operations team
    - delivery performance reports, churn analysis, and predictive models

#### **Determining SCD / Slowly Changing Dimension types for the tables**

**SCD Type 1 / Overwrite**

- `dim_product_category_name`
    - track changes in the names of the category
    - utilizing Type 1 / overwrite so as to simplify the overall process
    - historical tracking of category names are not significant because at the end of the day, the main analysis to be conducted are mainly about products, customers, sellers, locations, and periodicals.

**SCD Type 2 / Add New Row-Column**

- `dim_customers`
    - customers may change their location
    - because of this change, logistic fees and performance changes may affect future purchases
- `dim_sellers`
    - sellers may change their location
    - because of this change, 
- `dim_products`
    - changes in products may reflect changes in purchases
    - for example, the bigger the dimension may inflict higher freight cost, which may affect purchasing decision
    - also, the changes in price tends to affect purchasing decision also, especially if the customers are price-sensitive
    - hence, it is important to track price changes and dimension changes in relation to the total price & cost that the customers need to pay.

#### **ELT / Extract, Load, Transform with Python and SQL, and Luigi tasks**

The source of the database is in `olist-src` database, while the target database is in the `olist-dwh` database. In its current state, the `olist-dwh` database is empty without any schema or database. Hence, we need to design schemas and tables in the `olist-dwh` database.

1. Design `public` schema in data warehouse (using the same DDL and schema as the tables in the `olist-src` database)

2. Design `stg` / staging schema in `olist-dwh`(using similar schema to the `public` schema, but addressing the mistyped column names in the `public` schema)

3. Design `dwh` / data warehouse schema in `olist-dwh`, with dimension and fact tables. The design already addresses the SCD types for the required tables, and comprehensive fact tables to address the need for data analytics (sales performance and logistic performance).

4. Create an SQL script to fill the `dim.date` and `dim.time` tables in the `dwh` schema in `olist-dwh` database.

5. Create a python script to extract the data from `olist-src` to csv files, named `extract.py`.

6. Create SQL scripts to accomplish these functions:
  - transfer data from `public` schema to `stg` schema in `olist-dwh`
  - transfer data from `stg` schema to `dwh` schema in `olist-dwh`

7. Create a python script named `load.py` to accomplish these functions
  - read the csv files results from `extract.py` into dataframes
  - read the sql scripts to transfer from `public` schema to `stg` schema
  - clean the tables in `public` schema before being inserted new data
  - put the data from the dataframe previously into the `public` schema
  - transfer the data from the `public` schema to the `stg` schema

8. Create a python script named `transform.py` to transfer the data from `stg` schema to `dwh` schema

9. Combine all the scripts together inside `elt_main.py` to run all the scripts together.
