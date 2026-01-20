MODEL (
    name stg.wvbesl,
    kind FULL
);

WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.wvbesl
)
SELECT
    besluitnr,
    clientnr,
    _dlt_load_id
FROM raw.wvbesl
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
