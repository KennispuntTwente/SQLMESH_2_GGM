MODEL (
    name silver.client,
    kind FULL,
    description 'GGM-tabel van Sociaal Domein Beschikking en Voorziening - Domain Objects',
    grains rechtspersoon_id,
    references (rechtspersoon_id, gezagsdragergekend_enum_id)
);

-- Transform stg -> silver CLIENT (GGM schema)
-- Explicit casts to match GGM DDL types (INT for IDs)
SELECT
    CAST(clientnr AS INT) AS rechtspersoon_id,
    CAST(ind_gezag AS INT) AS gezagsdragergekend_enum_id,
    CAST(NULL AS VARCHAR(80)) AS code,
    CAST(NULL AS VARCHAR(80)) AS juridischestatus,
    CAST(NULL AS VARCHAR(80)) AS wettelijkevertegenwoordiging
FROM stg.szclient
