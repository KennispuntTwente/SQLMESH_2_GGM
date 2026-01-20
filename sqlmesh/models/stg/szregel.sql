MODEL (
    name stg.szregel,
    kind FULL
);

WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.szregel
)
SELECT
    kode_regeling,
    omschryving,
    _dlt_load_id
FROM raw.szregel
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
