MODEL (
    name silver.medewerker,
    kind FULL,
    description 'GGM-tabel van Sociaal Domein Beschikking en Voorziening - Domain Objects',
    grains medewerker_id,
    references extern_enum_id
);

-- Transform stg -> silver MEDEWERKER (GGM schema)
-- Explicit casts to match GGM DDL types (INT for IDs)
SELECT
    CAST(kode_werker AS INT) AS medewerker_id,
    CAST(naam AS VARCHAR(200)) AS achternaam,
    CAST(NULL AS DATE) AS datumindienst,
    CAST(NULL AS DATE) AS datumuitdienst,
    CAST(e_mail AS VARCHAR(255)) AS emailadres,
    CAST(NULL AS INT) AS extern_enum_id,
    CAST(kode_instan AS VARCHAR(50)) AS functie,
    CAST(ind_geslacht AS VARCHAR(255)) AS geslachtsaanduiding,
    CAST(kode_werker AS VARCHAR(255)) AS medewerkeridentificatie,
    CAST(toelichting AS VARCHAR(255)) AS medewerkertoelichting,
    CAST(NULL AS VARCHAR(255)) AS roepnaam,
    CAST(telefoon AS VARCHAR(20)) AS telefoonnummer,
    CAST(NULL AS VARCHAR(20)) AS voorletters,
    CAST(NULL AS VARCHAR(255)) AS voorvoegselachternaam
FROM stg.szwerker
