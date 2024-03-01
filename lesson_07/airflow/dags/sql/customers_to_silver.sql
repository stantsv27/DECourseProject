SELECT
        DISTINCT
        Id                                  AS client_id
      , FirstName                           AS first_name
      , LastName                            AS last_name
      , Email                               AS email
      , CASE
          WHEN RegistrationDate LIKE '%/%/%' THEN PARSE_DATE('%Y/%m/%d', RegistrationDate)
          WHEN LENGTH(SPLIT(RegistrationDate, '-')[OFFSET(1)]) = 3 THEN PARSE_DATE('%Y-%b-%d', RegistrationDate)
          ELSE CAST(RegistrationDate AS date)
        END                                 AS registration_date
      , State                               AS state
FROM    `de-07-stas-tsvietkov.bronze.customers_raw`;