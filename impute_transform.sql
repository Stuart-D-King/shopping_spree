/*
Stuart King
September 2017
Impute Missing Values and Output Final Table
SQL Queries using PostgreSQL
*/

/* Impute price and quantity values when missing or equal to zero */
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
/* Similar to how I imputed values for price and quantity, my imputation approach for missing total price values was to use the average total price based on the customer's shopping habits by retailer. If this imputation results in a null or zero value, I then calculate the average total price based solely on the retailer. Finally, if neither of the two above calculations yield a non-null and non-zero value, I use the global median as the imputed value. My assumption for this imputation approach is that customers will spend approximately the same amount by retailer. */

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


\copy final_output TO 'final_output.csv' DELIMITER ',' CSV HEADER
