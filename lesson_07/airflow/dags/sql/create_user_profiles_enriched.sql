CREATE OR REPLACE TABLE `de-07-stas-tsvietkov.gold.user_profiles_enriched` AS (
  SELECT
                sc.client_id
              , sc.email
              , up.full_name
              , sc.first_name
              , sc.last_name
              , sc.registration_date
              , up.birth_date
              , up.phone_number
              , sc.state
  FROM          `silver.customers`      AS sc
  LEFT JOIN     `silver.user_profiles`  AS up
                USING(email)
);