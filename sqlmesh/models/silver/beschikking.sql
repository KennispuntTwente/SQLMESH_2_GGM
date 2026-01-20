MODEL (
    name silver.beschikking,
    kind FULL,
    description 'GGM-tabel van Sociaal Domein Beschikking en Voorziening - Domain Objects',
    grains beschikking_id,
    references (client_id, heeft_voorzieningen_beschikte_voorziening_id, toewijzing_toewijzing_id, wet_enum_id)
);

-- Transform stg -> silver BESCHIKKING (GGM schema)
-- Explicit casts to match GGM DDL types (INT for IDs)
SELECT
    CAST(besluitnr AS INT) AS beschikking_id,
    CAST(clientnr AS INT) AS client_id,
    CAST(besluitnr AS INT) AS heeft_voorzieningen_beschikte_voorziening_id,
    CAST(NULL AS VARCHAR(20)) AS code,
    CAST(NULL AS VARCHAR(200)) AS commentaar,
    CAST(NULL AS DATE) AS datumafgifte,
    CAST(NULL AS INT) AS grondslagen,
    CAST(NULL AS INT) AS toewijzing_toewijzing_id,
    CAST(NULL AS INT) AS wet_enum_id
FROM stg.wvbesl
