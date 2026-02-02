MODEL (
    name stg.szukhis,
    kind FULL
);

WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.szukhis
)
SELECT
    uniekwvdos,
    bedrag,
    verslagnr,
    _dlt_load_id
FROM raw.szukhis
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
