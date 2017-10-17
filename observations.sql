/*
Stuart King
September 2017
General Observations
SQL Queries using PostgreSQL
*/

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
