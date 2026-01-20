MODEL (
    name stg.abc_refcod,
    kind FULL
);

WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.abc_refcod
)
SELECT
    code,
    domein,
    omschrijving,
    _dlt_load_id
FROM raw.abc_refcod
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
