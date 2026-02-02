MODEL (
    name stg.szclient,
    kind FULL
);

-- Extract latest load from raw layer
WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.szclient
)
SELECT
    clientnr,
    ind_gezag,
    _dlt_load_id
FROM raw.szclient
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
