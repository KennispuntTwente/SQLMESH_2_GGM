MODEL (
    name silver.wet_enum,
    kind FULL,
    description 'GGM-tabel van Sociaal Domein Beschikking en Voorziening - Domain Objects',
    grains wet_enum_id
);

-- Transform stg -> silver WET_ENUM (GGM schema)
-- Explicit casts to match GGM DDL types (INT for IDs)
SELECT
    CAST(kode_regeling AS INT) AS wet_enum_id,
    CAST(omschryving AS VARCHAR(255)) AS value
FROM stg.szregel
