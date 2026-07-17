CREATE OR REPLACE DATABASE ECOMMERCE_DB;
-- database creation

CREATE OR REPLACE SCHEMA ECOMMERCE_DB.BRONZE;
CREATE OR REPLACE SCHEMA ECOMMERCE_DB.SILVER; 
CREATE OR REPLACE SCHEMA ECOMMERCE_DB.GOLD;
-- creating medallion architecture model

USE DATABASE ECOMMERCE_DB;
-- use ecommerce_db as database

CREATE OR REPLACE TABLE BRONZE.CUSTOMERS(
    customer_id INT,
    full_name STRING, 
    email STRING,
    state STRING,
    city STRING
);
-- create customers table 

/*
INSERT INTO BRONZE.CUSTOMERS VALUES
(1,'Rafael Marques','rafael@email.com','RJ','Resende'),
(2,'Maria Silva','maria@email.com','SP','São Paulo'),
(3,'João Souza','joao@email.com','MG','Belo Horizonte'),
(4,'Ana Costa','ana@email.com','RJ','Volta Redonda'),
(5,'Carlos Lima','carlos@email.com','SP','Campinas');
-- insert static values into customers table
*/

CREATE OR REPLACE TABLE BRONZE.PRODUCTS (

    product_id INT,
    product_name STRING,
    category_id INT,
    price NUMBER(10,2)
);
-- create products table

/*
INSERT INTO BRONZE.PRODUCTS VALUES

(1,'Notebook',1,4500),
(2,'Mouse',2,90),
(3,'Teclado',2,180),
(4,'Monitor',1,1200),
(5,'Headset',2,250);
-- insert static values into products table
*/

CREATE OR REPLACE TABLE BRONZE.ORDERS (

    order_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    order_date DATE
);
-- create orders table


/*
INSERT INTO BRONZE.ORDERS VALUES

(1001,1,1,1,'2026-07-01'),
(1002,2,2,2,'2026-07-01'),
(1003,3,4,1,'2026-07-02'),
(1004,1,5,1,'2026-07-03'),
(1005,5,3,3,'2026-07-04'),
(1006,4,2,2,'2026-07-05');
-- insert static values into orders table
*/
