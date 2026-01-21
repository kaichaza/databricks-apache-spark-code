---------------------------------------------------------------------------------
-- Code updated in 2026 to run on the new free edition of Databricks
-- Parquet format now defaults to Delta
-- in mem table caching no longer supported on the free edition
-- underlying engine no longer based on Scala and JVM, now based on C++ Photon engine
---------------------------------------------------------------------------------
-- These queries runs on any test instance of Databricks using spark-sql
-- I tested it on Databricks Runtime 4.0, using Apache Spark 2.4, but it should
-- still run on all the newer releases of Apache Spark since none of the queries
-- are particularly complicated and are all based on spark core functionality
---------------------------------------------------------------------------------
-- All the queries are based largely on queries found in the book: 
--       Bill Chambers & Matei Zaharia , 
--       Spark: The Definitive Guide: Big Data Processing Made Simple, 
--       O'Reilly, 2018

------------------------------------------------------------------------------
--                    Apache Spark 2.x DataFrames and spark-sql
------------------------------------------------------------------------------
DROP TABLE IF EXISTS on_disk_table_products;

CREATE TABLE on_disk_table_products
(
    product_id INT,
    product_name STRING,  
    product_specs STRING, 
    price FLOAT 
);

INSERT INTO on_disk_table_products VALUES
(
    1, 'Apple iPhone 6', '16GB memory', 200  
);

INSERT INTO on_disk_table_products VALUES
(
    2, 'Samsung Galaxy 5', '16 megapixel camera', 100  
);

SELECT * FROM on_disk_table_products LIMIT 2;
--------------------------------------------------------------
--You can create a customer table from scratch or upload one from a csv file
--------------------------------------------------------------
DROP TABLE IF EXISTS on_disk_table_customers;

CREATE TABLE on_disk_table_customers
(
    customer_id INT,
    customer_name STRING,  
    address STRING, 
    geolocation_x FLOAT,
    geolocation_y FLOAT 
);

INSERT INTO on_disk_table_customers VALUES
(
    1, 'Pete', 'North Road', 5, 10  
);

INSERT INTO on_disk_table_customers VALUES
(
    2, 'Tanya', 'South Road', 2, 0  
);

SELECT * FROM on_disk_table_customers LIMIT 2;
--------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW in_memory_table_customers AS
SELECT
    customer_id,
    customer_name,
    address,
    geolocation_x,
    geolocation_y
FROM on_disk_table_customers;

SELECT * FROM in_memory_table_customers LIMIT 2;

EXPLAIN SELECT * FROM in_memory_table_customers LIMIT 2;

--CACHE TABLE in_memory_table_customers;

EXPLAIN SELECT * FROM in_memory_table_customers LIMIT 2;
--------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW in_memory_table_products AS
SELECT
    product_id,
    product_name,  
    product_specs, 
    price
FROM on_disk_table_products LIMIT 2;

SELECT * FROM in_memory_table_products LIMIT 2;

EXPLAIN SELECT * FROM in_memory_table_products LIMIT 2;

--CACHE TABLE in_memory_table_products;

EXPLAIN SELECT * FROM in_memory_table_products LIMIT 2;

--------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW in_memory_table_orders AS
SELECT
    customer_id,
    customer_name,
    address,
    geolocation_x,
    geolocation_y,
        CASE
            WHEN customer_id = 1 THEN 1
            WHEN customer_id = 2 THEN 2
        END AS product_id,
        CASE
            WHEN customer_id = 1 THEN 3
            WHEN customer_id = 2 THEN 2
        END AS quantity_ordered,
            5.0 AS warehouse_x,
            4.0 AS warehouse_y,
            10 AS shipping_cost_per_km
FROM in_memory_table_customers;

--CACHE TABLE in_memory_table_orders;

SELECT * FROM in_memory_table_orders LIMIT 2;
--------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW in_memory_table_invoices AS
SELECT
    customer_id,
    customer_name,
    address,
    geolocation_x,
    geolocation_y,
    warehouse_x,
    warehouse_y,
    product_name,
    product_specs,
    price AS price_per_unit,
    quantity_ordered,
    price * quantity_ordered AS total_price,
    SQRT(  POWER(geolocation_x - warehouse_x,2)
         + POWER(geolocation_y - warehouse_y,2)  ) AS shipping_distance,
    SQRT(  POWER(geolocation_x - warehouse_x,2)
         + POWER(geolocation_y - warehouse_y,2)  ) * shipping_cost_per_km AS shipping_costs
FROM in_memory_table_orders
INNER JOIN
    in_memory_table_products
ON
    in_memory_table_orders.product_id = in_memory_table_products.product_id;

--CACHE TABLE in_memory_table_invoices;

SELECT * FROM in_memory_table_invoices LIMIT 2;

EXPLAIN SELECT * FROM in_memory_table_invoices LIMIT 2;

--------------------------------------------------------------  
SELECT
    SUM(quantity_ordered) AS total_quantity_ordered_aggregate,
    AVG(shipping_costs) AS average_shipping_costs,
    SUM(shipping_costs) AS total_shipping_costs,    
    SUM(total_price) AS total_revenue_to_shop
FROM
    in_memory_table_invoices;

-------------------------------------------------------------
CREATE TABLE on_disk_table_invoices_spark_sql
AS
SELECT * FROM in_memory_table_invoices;

SELECT * FROM on_disk_table_invoices_spark_sql LIMIT 2;
--------------------------------------------------------------  
