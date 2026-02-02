MODEL (
    name stg.wvdos,
    kind FULL
);

WITH latest_load AS (
    SELECT MAX(_dlt_load_id) AS load_id FROM raw.wvdos
)
SELECT
    besluitnr,
    volgnr_ind,
    uniek,
    kode_reden_einde_voorz,
    _dlt_load_id
FROM raw.wvdos
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
