MODEL (
    name silver.declaratieregel,
    kind FULL,
    description 'GGM-tabel van Sociaal Domein Beschikking en Voorziening - Domain Objects',
    grains declaratieregel_id,
    references (betreft_client_id, is_voor_beschikking_id, valt_binnen_declaratie_id)
);

-- Transform stg -> silver DECLARATIEREGEL (GGM schema)
-- Explicit casts to match GGM DDL types (INT for IDs)
-- Joins: wvind_b -> wvdos -> szukhis
SELECT
    CAST(wd.uniek AS INT) AS declaratieregel_id,
    su.bedrag AS bedrag,
    CAST(wi.clientnr AS INT) AS betreft_client_id,
    CAST(NULL AS VARCHAR(20)) AS code,
    CAST(NULL AS DATE) AS datumeinde,
    CAST(NULL AS DATE) AS datumstart,
    CAST(wi.besluitnr AS INT) AS is_voor_beschikking_id,
    CAST(su.verslagnr AS INT) AS valt_binnen_declaratie_id
FROM stg.wvind_b wi
INNER JOIN stg.wvdos wd ON wi.besluitnr = wd.besluitnr AND wi.volgnr_ind = wd.volgnr_ind
INNER JOIN stg.szukhis su ON su.uniekwvdos = wd.uniek
