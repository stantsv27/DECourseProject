MERGE `gold.user_profiles_enriched` AS ue
USING `silver.user_profiles`        AS up
ON ue.email = up.email
WHEN MATCHED AND (ue.state IS NULL OR ue.first_name IS NULL OR ue.last_name IS NULL) THEN
  UPDATE SET  state = COALESCE(up.state, ue.state)
            , first_name = COALESCE(SPLIT(up.full_name, ' ')[OFFSET(0)], ue.first_name)
            , last_name = COALESCE(SPLIT(up.full_name, ' ')[OFFSET(1)], ue.last_name);