SELECT
        CustomerId                                                      AS client_id
      , CASE
          WHEN PurchaseDate LIKE '%/%/%' THEN PARSE_DATE('%Y/%m/%d', PurchaseDate)
          WHEN LENGTH(SPLIT(PurchaseDate, '-')[OFFSET(1)]) = 3 THEN PARSE_DATE('%Y-%b-%d', PurchaseDate)
          ELSE CAST(PurchaseDate AS date)
        END                                                             AS purchase_date
      , UPPER(Product)                                                  AS product_name
      , CASE
          WHEN Price LIKE '%$' THEN CAST(LEFT(Price, LENGTH(Price) - 1) AS FLOAT64)
          WHEN Price LIKE '%USD' THEN CAST(LEFT(Price, LENGTH(Price) - 3) AS FLOAT64)
          ELSE CAST(Price AS FLOAT64)
        END                                                             AS price
FROM `de-07-stas-tsvietkov.bronze.sales_raw`;