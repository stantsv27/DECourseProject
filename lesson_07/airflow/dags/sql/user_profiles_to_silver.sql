SELECT
            email
          , full_name
          , state
          , CAST(birth_date AS date) AS birth_date
          , phone_number
FROM      `de-07-stas-tsvietkov.bronze.user_profile_raw`;