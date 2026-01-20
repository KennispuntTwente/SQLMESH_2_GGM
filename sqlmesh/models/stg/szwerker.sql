MODEL (
    name stg.szwerker,
    kind FULL
);

WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.szwerker
)
SELECT
    kode_werker,
    naam,
    kode_instan,
    e_mail,
    ind_geslacht,
    toelichting,
    telefoon,
    _dlt_load_id
FROM raw.szwerker
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
