/*
Stuart King
September 18, 2017
Answers to Ibotta's Analyst Exercise
SQL Queries using PostgreSQL
*/

/* Create the database and tables, copy CSV files to newly created tables, change '(null)' values to NULL, and update data types for numeric columns */

CREATE DATABASE ibotta;

CREATE TABLE receipts
(id varchar, customer_id varchar, retailer_id varchar, created_at timestamp with time zone, total_price varchar);

\copy receipts FROM '~/Desktop/ibotta/data/receipts.csv' DELIMITER ',' CSV HEADER

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

\copy customers FROM '~/Desktop/ibotta/data/customers.csv' DELIMITER ',' CSV HEADER

UPDATE customers
SET education = NULL
WHERE education = 'none';


CREATE TABLE retailers
(id varchar, retailer_type varchar);

\copy retailers FROM '~/Desktop/ibotta/data/retailers.csv' DELIMITER ',' CSV HEADER


CREATE TABLE brands
(id varchar, name varchar);

\copy brands FROM '~/Desktop/ibotta/data/brands.csv' DELIMITER ',' CSV HEADER

UPDATE brands
SET name = 'No Info'
WHERE name = 'Needs Verification';


CREATE TABLE product_categories
(id varchar, name varchar);

\copy product_categories FROM '~/Desktop/ibotta/data/product_categories.csv' DELIMITER ',' CSV HEADER


CREATE TABLE receipt_item_details
(receipt_item_id varchar, receipt_id varchar, primary_category_id varchar, secondary_category_id varchar, tertiary_category_id varchar, brand_id varchar, global_product_id varchar);

\copy receipt_item_details FROM '~/Desktop/ibotta/data/receipt_item_details.csv' DELIMITER ',' CSV HEADER

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

\copy receipt_items FROM '~/Desktop/ibotta/data/receipt_items.csv' DELIMITER ',' CSV HEADER

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

/* -------------- */

/* SECTION I: SQL FUNCTIONS */

/* 1. Which customer (customer_id) submitted the most receipts? */

SELECT c.id, COUNT(*) AS cnt
FROM customers AS c
JOIN receipts AS r
ON c.id = r.customer_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

/* 2. Provide a list of customer_ids that have submitted 3 or more receipts. */

SELECT c.id
FROM customers AS c
JOIN receipts AS r
ON c.id = r.customer_id
GROUP BY 1
HAVING COUNT(*) >= 3;

/* 3. Provide me a list of customer_ids with the receipt that has the largest receipt amount (total_price) for each customer. */

SELECT a.customer_id, a.receipt_id, a.total_price
FROM (
  SELECT c.id AS customer_id, r.id AS receipt_id, r.total_price, RANK() OVER(PARTITION BY c.id ORDER BY r.total_price DESC) AS receipt_rank
  FROM customers AS c
  JOIN receipts AS r
  ON c.id = r.customer_id
  WHERE r.total_price IS NOT NULL AND r.total_price <> 0
  GROUP BY 1,2,3) AS a
WHERE receipt_rank = 1
ORDER BY 1;


/* 4. Provide a list of customer_ids and a comma separated list of retailer_ids that they have shopped at. */

SELECT c.id AS customer_id, string_agg(DISTINCT(r.retailer_id), ',') AS stores
FROM customers AS c
JOIN receipts AS r
ON c.id = r.customer_id
GROUP BY 1;

/* 1. What is an example of needing to use a cross join?
Because a cross join is useful for when we want to combine every row from one table with every row from another table, an example could be for when we want to create a report of all customer and product combinations. Cross joining a customer table with a product table would result in a dataset of every combination of customer and product. We could then add new columns to the result table to determine if a customer purchased a particular product, or the quantity of each product purchased. */


/* 2. What is an example of needing to join a table to itself?
A self join could be useful if you had a products table that included a column that indicates the id number of another product for which the product is a set. For example, assume a product table with the following columns: id, name, cateogry, set_id. You could then write the following query to return a dataset of each product and the paired product that completes the set.

  SELECT p.name, s.name AS set_item,
  FROM product AS p
  LEFT JOIN product AS s
  ON p.id = s.set_id;

A left join is used here to return all products, regardless of whether or not the product is part of a set. */

/* -------------- */

/* SECTION II:  DATA MANIPULATION */

/* Remove extreme outliers first */

UPDATE receipt_items
SET price = 0
WHERE price > 10000;

UPDATE receipts
SET total_price = 0
WHERE total_price > 100000;


/* Using the standard deviation for price and total price, set values above or below three standard deviations from the mean to zero. This step is intended to remove outliers from the data. */
/* THIS IS DONE IMMEDIATELY AFTER CREATING THE TABLES AND REMOVING EXTREME OUTLIERS. MULTIPLE QUERY EXECUTIONS SHOULD NOT BE PERFORMED. */

WITH price_bounds AS (
    SELECT (AVG(price) - STDDEV_SAMP(price) * 3) AS lower_bound,
           (AVG(price) + STDDEV_SAMP(price) * 3) AS upper_bound
    FROM receipt_items)

    UPDATE receipt_items
    SET price = 0
    WHERE price > (SELECT upper_bound FROM price_bounds) OR price < (SELECT lower_bound FROM price_bounds);

WITH total_price_bounds AS (
    SELECT (AVG(total_price) - STDDEV_SAMP(total_price) * 3) AS lower_bound,
           (AVG(total_price) + STDDEV_SAMP(total_price) * 3) AS upper_bound
    FROM receipts)

    UPDATE receipts
    SET total_price = 0
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


/* Impute price and quantity values when missing or equal to 0 */
/* First a temporary table is created that joins the receipt_items and receipt_item_details tables, keeping the variables we will need to help impute missing values. For price, the overarching assumption for making the below imputations is that a good approximation of a product's missing value is the average price of similar products. I first calculate the average price of the global product ID for which the product belongs to. If that value is either null or zero, I then calculate the average price based on the product's primary and secondary category classifications. If the imputed value is still either null or zero, I then calculate the average based on the product's retailer and brand IDs. Finally, if the imputed value is still null or zero, I use the global median price as the product's imputed value.

A similar process is followed for calculating missing product quantities. The assumption here is that similar products are bought in similar quantities. */

CREATE TEMPORARY TABLE new_receipt_items AS
  WITH s AS (
    SELECT r.id, ri.receipt_item_id, rid.global_product_id, rid.primary_category_id, rid.secondary_category_id, rid.tertiary_category_id, ri.price, ri.median_price, ri.quantity, ri.median_qty, rid.brand_id, r.retailer_id
    FROM receipt_items AS ri
    JOIN receipt_item_details AS rid
    ON ri.receipt_item_id = rid.receipt_item_id
    JOIN receipts AS r
    ON rid.receipt_id = r.id)

    SELECT s.id AS receipt_id, s.receipt_item_id, s.global_product_id, s.retailer_id, s.brand_id, s.primary_category_id, s.secondary_category_id, s.tertiary_category_id, s.price,

      CASE
      WHEN s.price <> 0 AND s.price IS NOT NULL <> 0 THEN s.price

      WHEN ((s.price = 0 OR s.price IS NULL) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) IS NOT NULL AND ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) <> 0)) THEN ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2)

      WHEN ((s.price = 0 OR s.price IS NULL) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) IS NULL OR ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) = 0) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2) IS NOT NULL) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2) <> 0)) THEN ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2)

      WHEN ((s.price = 0 OR s.price IS NULL) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) IS NULL OR ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) = 0) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2) IS NULL OR ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2) = 0) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.retailer_id, s.brand_id),2) IS NOT NULL) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.retailer_id, s.brand_id),2) <> 0)) THEN ROUND(AVG(s.price) OVER(PARTITION BY s.retailer_id, s.brand_id),2)

      WHEN ((s.price = 0 OR s.price IS NULL) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) IS NULL OR ROUND(AVG(s.price) OVER(PARTITION BY s.global_product_id),2) = 0) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2) IS NULL OR ROUND(AVG(s.price) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),2) = 0) AND (ROUND(AVG(s.price) OVER(PARTITION BY s.retailer_id, s.brand_id),2) IS NULL OR ROUND(AVG(s.price) OVER(PARTITION BY s.retailer_id, s.brand_id),2) = 0)) THEN s.median_price

      END AS imputed_price,

      CASE
      WHEN s.price = 0 OR s.price IS NULL THEN 'yes'
      ELSE 'no'
      END AS flag_price_imputed,

      s.quantity,

      CASE
      WHEN s.quantity <> 0 AND s.quantity IS NOT NULL THEN s.quantity

      WHEN ((s.quantity = 0 OR s.quantity IS NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) IS NOT NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) <> 0)) THEN ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0)

      WHEN ((s.quantity = 0 OR s.quantity IS NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) IS NULL OR ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) = 0) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0) IS NOT NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0) <> 0)) THEN ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0)

      WHEN ((s.quantity = 0 OR s.quantity IS NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) IS NULL OR ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) = 0) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0) IS NULL OR ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0) = 0) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.retailer_id, s.brand_id),0) IS NOT NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.retailer_id, s.brand_id),0) <> 0)) THEN ROUND(AVG(s.quantity) OVER(PARTITION BY s.retailer_id, s.brand_id),0)

      WHEN ((s.quantity = 0 OR s.quantity IS NULL) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) IS NULL OR ROUND(AVG(s.quantity) OVER(PARTITION BY s.global_product_id),0) = 0) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0) IS NULL OR ROUND(AVG(s.quantity) OVER(PARTITION BY s.primary_category_id, s.secondary_category_id),0) = 0) AND (ROUND(AVG(s.quantity) OVER(PARTITION BY s.retailer_id, s.brand_id),0) IS NULL OR ROUND(AVG(s.quantity) OVER(PARTITION BY s.retailer_id, s.brand_id),0) = 0)) THEN s.median_qty

      END AS imputed_quantity,

      CASE
      WHEN s.quantity = 0 OR s.quantity IS NULL THEN 'yes'
      ELSE 'no'
      END AS flag_qty_imputed

    FROM s;


/* Update the median total price column in the receipts table */

UPDATE receipts SET median_total_price = (
  SELECT ROUND(AVG(total_price),2)
      FROM (
        SELECT total_price
        FROM (
          SELECT total_price
          FROM receipts
          WHERE total_price IS NOT NULL AND total_price <> 0
          ORDER BY total_price ASC
          LIMIT (
            SELECT ROUND(COUNT(*) / 2,0)
            FROM receipts
            WHERE total_price IS NOT NULL AND total_price <> 0)) AS temp1
        ORDER BY total_price DESC
        LIMIT 2) AS temp2);


/* Impute total price when missing or equal to 0 */
/* Similar to how I imputed values for price and quantity, my imputation approach for missing total price values was to use the average total price based on the customer's shopping habits by retailer. If this imputation results in a null or zero value, I then calculate the average total price based soley on the retailer. Finally, if neither of the two above calculations yield a non-null and non-zero value, I use the global median as the imputed value. My assumption for this imputation approach is that customers will spend approximately the same amount by retailer. */

CREATE TEMPORARY TABLE new_receipts AS
  SELECT r.*,
    CASE
    WHEN r.total_price <> 0 AND r.total_price IS NOT NULL THEN r.total_price

    WHEN ((r.total_price = 0 OR r.total_price IS NULL) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2) IS NOT NULL) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2) <> 0)) THEN ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2)

    WHEN ((r.total_price = 0 OR r.total_price IS NULL) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2) IS NULL OR ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2) = 0) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.retailer_id),2) IS NOT NULL) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.retailer_id),2) <> 0)) THEN ROUND(AVG(r.total_price) OVER (PARTITION BY r.retailer_id),2)

    WHEN ((r.total_price = 0 OR r.total_price IS NULL) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2) IS NULL OR ROUND(AVG(r.total_price) OVER (PARTITION BY r.customer_id, r.retailer_id),2) = 0) AND (ROUND(AVG(r.total_price) OVER (PARTITION BY r.retailer_id),2) IS NULL OR ROUND(AVG(r.total_price) OVER (PARTITION BY r.retailer_id),2) = 0)) THEN r.median_total_price

    END AS imputed_total_price,

    CASE
    WHEN r.total_price = 0 OR r.total_price IS NULL THEN 'yes'
    ELSE 'no'
    END AS flag_total_price_imputed

  FROM receipts AS r;


/* Create a final output table and save to a CSV file */

CREATE TABLE final_output AS
  SELECT c.id AS customer_id, c.gender, date_part('year', age(c.birth_date)) AS age, c.education, c.state, nr.retailer_id, rt.retailer_type, nr.id AS receipt_id, nr.imputed_total_price AS total_price, nr.created_at AT TIME ZONE 'MST' AS created_at_mst, nri.receipt_item_id, nri.primary_category_id, nri.secondary_category_id, nri.tertiary_category_id, b.name AS brand_name, nri.global_product_id, nri.imputed_price AS price, nri.imputed_quantity AS quantity, nri.flag_price_imputed, nri.flag_qty_imputed
  FROM new_receipt_items AS nri
  LEFT JOIN new_receipts AS nr
  ON nri.receipt_id = nr.id
  LEFT JOIN customers AS c
  ON nr.customer_id = c.id
  LEFT JOIN retailers AS rt
  ON nr.retailer_id = rt.id
  LEFT JOIN brands AS b
  ON nri.brand_id = b.id;


\copy final_output TO '~/Desktop/ibotta/Final_Output_StuartKing.csv' DELIMITER ',' CSV HEADER


/* -------------- */

/* SECTION II: OBSERVATIONS */

/* Calculate the average consumer age */
/* 37.4 years */
WITH temp AS (
  SELECT DISTINCT(customer_id), age
  FROM final_output)
    SELECT AVG(temp.age)
    FROM temp
    WHERE age <= 90;


/* Average age by gender */
/* 37.7 for females, 35.8 for males */
WITH temp AS (
  SELECT DISTINCT(customer_id), gender, age
  FROM final_output)
    SELECT DISTINCT(gender), AVG(age) AS average_age
    FROM temp
    WHERE age <= 90
    GROUP BY 1;


/* Calculate gender percentages

gender | gender_count | gender_percent
--------+--------------+----------------
F      |         1894 |          80.42
M      |          289 |          12.27
       |          172 |           7.30
*/
WITH temp AS (
  SELECT DISTINCT(customer_id), gender
  FROM final_output)

  SELECT DISTINCT(gender),
     COUNT(gender) AS gender_count,
     ROUND(COUNT(gender) * 100.0 / (SELECT COUNT(*) FROM temp),2) AS gender_percent
  FROM temp
  GROUP BY 1;


/* Calculate the top 5 states by proportion of users

state | state_count | state_percent
-------+-------------+---------------
TX    |         185 |          7.86
NC    |         151 |          6.41
PA    |         150 |          6.37
FL    |         149 |          6.33
OH    |         137 |          5.82
*/
WITH temp AS (
  SELECT DISTINCT(customer_id), state
  FROM final_output)

  SELECT DISTINCT(state),
     COUNT(state) AS state_count,
     ROUND(COUNT(state) * 100.0 / (SELECT COUNT(*) FROM temp),2) AS state_percent
  FROM temp
  GROUP BY 1
  ORDER BY state_percent DESC;


/* Top 5 states by average spend

state | average_spend
-------+---------------
ME    |        129.52
AR    |        119.94
ND    |        113.83
VT    |        106.12
LA    |        105.52
*/
WITH temp AS (
  SELECT DISTINCT(customer_id), state, total_price
  FROM final_output)

  SELECT DISTINCT(state), ROUND(AVG(total_price),2) AS average_spend
  FROM temp
  GROUP BY 1
  ORDER BY average_spend DESC;


/* Top 10 brands purchased

brand_name                            | brand_count | brand_percent
--------------------------------------+-------------+---------------
No Info                               |       23052 |          9.58
Generic Produce                       |       21060 |          8.75
Great Value                           |        9354 |          3.89
Coupon                                |        4726 |          1.96
Food Lion                             |        3995 |          1.66
Kroger                                |        2660 |          1.11
Campbell's                            |        2286 |          0.95
Kraft                                 |        1996 |          0.83
Yoplait Original                      |        1797 |          0.75
Weis                                  |        1525 |          0.63
*/
SELECT DISTINCT(brand_name),
   COUNT(brand_name) AS brand_count,
   ROUND(COUNT(brand_name) * 100.0 / (SELECT COUNT(*) FROM final_output),2) AS brand_percent
FROM final_output
GROUP BY 1
ORDER BY brand_percent DESC;


/* Proportion of consumers by education

education                | education_count | education_percent
-------------------------+-----------------+-------------------
                         |             920 |             39.07
Bachelor Degree          |             339 |             14.39
Some College (No Degree) |             335 |             14.23
High School              |             223 |              9.47
Associates               |             193 |              8.20
Masters                  |             163 |              6.92
College                  |              46 |              1.95
Other                    |              43 |              1.83
Professional Degree      |              39 |              1.66
Doctorate                |              26 |              1.10
Graduate School          |              10 |              0.42
*/
WITH temp AS (
  SELECT DISTINCT(customer_id), education
  FROM final_output)

  SELECT DISTINCT(education),
     COUNT(education) AS education_count,
     ROUND(COUNT(education) * 100.0 / (SELECT COUNT(*) FROM temp),2) AS education_percent
  FROM temp
  GROUP BY 1
  ORDER BY education_percent DESC;


/* Proportion of purchases by retailer type

retailer_type        | retailer_count | retailer_percent
---------------------+----------------+------------------
Grocery              |         234800 |            97.61
Pharmacy             |           4062 |             1.69
Convenience          |            729 |             0.30
Dollar Store         |            559 |             0.23
Beer, Wine & Spirits |            278 |             0.12
Arts & Crafts        |             49 |             0.02
Restaurant           |             53 |             0.02
Electronics          |              2 |             0.00
Home Improvement     |              7 |             0.00
Mass Merchandise     |              9 |             0.00
Pet                  |              2 |             0.00
Toy                  |              7 |             0.00
*/
SELECT DISTINCT(retailer_type),
   COUNT(retailer_type) AS retailer_count,
   ROUND(COUNT(retailer_type) * 100.0 / (SELECT COUNT(*) FROM final_output),2) AS retailer_percent
FROM final_output
GROUP BY 1
ORDER BY retailer_percent DESC;


/* Proportion of receipts by retailer type

retailer_type     | retailer_count | retailer_percent
----------------------+----------------+------------------
 Grocery              |          18289 |            89.02
 Pharmacy             |           1256 |             6.11
 Convenience          |            501 |             2.44
 Dollar Store         |            215 |             1.05
 Beer, Wine & Spirits |            204 |             0.99
 Restaurant           |             48 |             0.23
 Arts & Crafts        |             16 |             0.08
 Mass Merchandise     |              7 |             0.03
 Toy                  |              4 |             0.02
 Home Improvement     |              2 |             0.01
 Pet                  |              2 |             0.01
 Electronics          |              1 |             0.00
*/
WITH temp AS (
  SELECT DISTINCT(receipt_id), retailer_type
  FROM final_output)

  SELECT DISTINCT(retailer_type),
     COUNT(retailer_type) AS retailer_count,
     ROUND(COUNT(retailer_type) * 100.0 / (SELECT COUNT(*) FROM temp),2) AS retailer_percent
  FROM temp
  GROUP BY 1
  ORDER BY retailer_percent DESC;
