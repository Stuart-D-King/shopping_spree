import psycopg2

'''
Stuart King
September 2017

A SQL pipeline for creating a final output table. The below queries are written using PostreSQL. The pipeline assumes the database and accompanying tables have been previously created, and table manipulations (e.g. removing outliers, adding new median value columns to tables, etc.) have been performed. Queries to create the database and tables are included in my create_db.sql file.
'''

conn = psycopg2.connect(dbname='shopping_spree', user='stuartking', host='/tmp')
c = conn.cursor()


# Update the median price column in the receipt_items table
c.execute(
    '''UPDATE receipt_items SET median_price = (
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
    LIMIT 2) AS temp2);'''
)


# Update the median quantity column in the receipt_items table
c.execute(
    '''UPDATE receipt_items SET median_qty = (
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
    LIMIT 2) AS temp2);'''
)

conn.commit()


# Impute item price and quantity
c.execute(
    '''CREATE TEMPORARY TABLE new_receipt_items AS
    WITH s AS (
        SELECT r.id, ri.receipt_item_id, rid.global_product_id, rid.primary_category_id, rid.secondary_category_id, rid.tertiary_category_id, ri.price, ri.median_price, ri.quantity, ri.median_qty, rid.brand_id, r.retailer_id
        FROM receipt_items AS ri
        JOIN receipt_item_details AS rid
        ON ri.receipt_item_id = rid.receipt_item_id
        JOIN receipts AS r
        ON rid.receipt_id = r.id)

        SELECT s.id AS receipt_id, s.receipt_item_id, s.global_product_id, s.retailer_id, s.brand_id, s.primary_category_id, s.secondary_category_id, s.tertiary_category_id, s.price,

            CASE
            WHEN s.price <> 0 AND s.price IS NOT NULL THEN s.price

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

        FROM s;'''
)

conn.commit()


# Update the median total price column in the receipts table
c.execute(
    '''UPDATE receipts SET median_total_price = (
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
        LIMIT 2) AS temp2);'''
)

conn.commit()


# Impute total receipt price
c.execute(
    '''CREATE TEMPORARY TABLE new_receipts AS
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

    FROM receipts AS r;'''
)

conn.commit()


# Drop the final output table if it already exists
c.execute(
    '''DROP TABLE IF EXISTS final_output;'''
)

# Create the final output table
c.execute(
    '''CREATE TABLE final_output AS
    SELECT c.id AS customer_id, c.gender, date_part('year', age(c.birth_date)) AS age, c.education, c.state, nr.retailer_id, rt.retailer_type, nr.id AS receipt_id, nr.imputed_total_price AS total_price, nr.created_at AT TIME ZONE 'MST' AS created_at_mst, nri.receipt_item_id, nri.primary_category_id, nri.secondary_category_id, nri.tertiary_category_id, b.name AS brand_name, nri.global_product_id, nri.imputed_price AS price, nri.imputed_quantity AS quantity, nri.flag_price_imputed, nri.flag_qty_imputed

    FROM new_receipt_items AS nri
    LEFT JOIN new_receipts AS nr
    ON nri.receipt_id = nr.id
    LEFT JOIN customers AS c
    ON nr.customer_id = c.id
    LEFT JOIN retailers AS rt
    ON nr.retailer_id = rt.id
    LEFT JOIN brands AS b
    ON nri.brand_id = b.id;'''
)

conn.commit()
conn.close()
