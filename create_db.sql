/*
Stuart King
September 2017
Create Database
SQL Queries using PostgreSQL
*/

/* Create the database and tables, copy CSV files to newly created tables, change '(null)' values to NULL, and update data types for numeric columns */

CREATE DATABASE shopping_spree;

CREATE TABLE receipts
(id varchar, customer_id varchar, retailer_id varchar, created_at timestamp with time zone, total_price varchar);

\copy receipts FROM 'data/receipts.csv' DELIMITER ',' CSV HEADER

UPDATE receipts
SET total_price = NULL
WHERE total_price = '(null)';

ALTER TABLE receipts
ALTER COLUMN total_price TYPE numeric USING (total_price::numeric);

/* Add a new column to the receipts table to hold the median total price of all receipts */
ALTER TABLE receipts
ADD median_total_price numeric;


CREATE TABLE customers
(id varchar, gender char(1), birth_date date, education varchar, state char(2));

\copy customers FROM 'data/customers.csv' DELIMITER ',' CSV HEADER

UPDATE customers
SET education = NULL
WHERE education = 'none';


CREATE TABLE retailers
(id varchar, retailer_type varchar);

\copy retailers FROM 'data/retailers.csv' DELIMITER ',' CSV HEADER


CREATE TABLE brands
(id varchar, name varchar);

\copy brands FROM 'data/brands.csv' DELIMITER ',' CSV HEADER

UPDATE brands
SET name = 'No Info'
WHERE name = 'Needs Verification';


CREATE TABLE product_categories
(id varchar, name varchar);

\copy product_categories FROM 'data/product_categories.csv' DELIMITER ',' CSV HEADER


CREATE TABLE receipt_item_details
(receipt_item_id varchar, receipt_id varchar, primary_category_id varchar, secondary_category_id varchar, tertiary_category_id varchar, brand_id varchar, global_product_id varchar);

\copy receipt_item_details FROM 'data/receipt_item_details.csv' DELIMITER ',' CSV HEADER

UPDATE receipt_item_details
SET primary_category_id = NULL
WHERE primary_category_id = '(null)';

UPDATE receipt_item_details
SET secondary_category_id = NULL
WHERE secondary_category_id = '(null)';

UPDATE receipt_item_details
SET tertiary_category_id = NULL
WHERE tertiary_category_id = '(null)';

UPDATE receipt_item_details
SET brand_id = NULL
WHERE brand_id = '(null)';


CREATE TABLE receipt_items
(receipt_item_id varchar, price varchar, quantity varchar);

\copy receipt_items FROM 'data/receipt_items.csv' DELIMITER ',' CSV HEADER

UPDATE receipt_items
SET price = NULL
WHERE price = '(null)';

ALTER TABLE receipt_items
ALTER COLUMN price TYPE numeric USING (price::numeric);

UPDATE receipt_items
SET quantity = NULL
WHERE quantity = '(null)';

ALTER TABLE receipt_items
ALTER COLUMN quantity TYPE numeric USING (quantity::numeric);

/* Add a new column to the receipt_items table to hold the median price of all products purchased */
ALTER TABLE receipt_items
ADD median_price numeric;

/* Add a new column to the receipt_items table to hold the median quantity of all products purchased */
ALTER TABLE receipt_items
ADD median_qty numeric;

/* Remove extreme outliers */

UPDATE receipt_items
SET price = NULL
WHERE price > 10000;

UPDATE receipts
SET total_price = NULL
WHERE total_price > 100000;


/* Using the standard deviation for price and total price, set values above or below three standard deviations the mean to zero. This step is intended to remove outliers from the data. */
/* THIS IS DONE IMMEDIATELY AFTER CREATING THE TABLES AND REMOVING EXTREME OUTLIERS. MULTIPLE QUERY EXECUTIONS SHOULD NOT BE PERFORMED. */

WITH price_bounds AS (
    SELECT (AVG(price) - STDDEV_SAMP(price) * 3) AS lower_bound,
           (AVG(price) + STDDEV_SAMP(price) * 3) AS upper_bound
    FROM receipt_items)

    UPDATE receipt_items
    SET price = NULL
    WHERE price > (SELECT upper_bound FROM price_bounds) OR price < (SELECT lower_bound FROM price_bounds);

WITH total_price_bounds AS (
    SELECT (AVG(total_price) - STDDEV_SAMP(total_price) * 3) AS lower_bound,
           (AVG(total_price) + STDDEV_SAMP(total_price) * 3) AS upper_bound
    FROM receipts)

    UPDATE receipts
    SET total_price = NULL
    WHERE total_price > (SELECT upper_bound FROM total_price_bounds) OR total_price < (SELECT lower_bound FROM total_price_bounds);


/* Update the median price column in the receipt_items table */

UPDATE receipt_items SET median_price = (
  SELECT ROUND(AVG(price),2)
      FROM (
        SELECT price
        FROM (
          SELECT price
          FROM receipt_items
          WHERE price IS NOT NULL AND price <> 0
          ORDER BY price ASC
          LIMIT (
            SELECT ROUND(COUNT(*) / 2,0)
            FROM receipt_items
            WHERE price IS NOT NULL AND price <> 0)) AS temp1
        ORDER BY price DESC
        LIMIT 2) AS temp2);


/* Update the median quantity column in the receipt_items table */

UPDATE receipt_items SET median_qty = (
  SELECT ROUND(AVG(quantity),0)
      FROM (
        SELECT quantity
        FROM (
          SELECT quantity
          FROM receipt_items
          WHERE quantity IS NOT NULL AND quantity > 0
          ORDER BY quantity ASC
          LIMIT (
            SELECT ROUND(COUNT(*) / 2,0)
            FROM receipt_items
            WHERE quantity IS NOT NULL AND quantity > 0)) AS temp1
        ORDER BY quantity DESC
        LIMIT 2) AS temp2);
