MODEL (
    name silver.beschikte_voorziening,
    kind FULL,
    description 'GGM-tabel van Sociaal Domein Beschikking en Voorziening - Domain Objects',
    grains beschikte_voorziening_id,
    references (eenheid_enum_id, frequentie_enum_id, heeft_leveringsvorm_293_id, is_voorziening_voorziening_id, leveringsvorm_287_enum_id, toegewezen_product_toewijzing_id, wet_enum_id)
);

-- Transform stg -> silver BESCHIKTE_VOORZIENING (GGM schema)
-- Explicit casts to match GGM DDL types (INT for IDs)
-- Joins: wvind_b -> szregel, wvbesl, wvdos, abc_refcod
SELECT
    CAST(CONCAT(wi.besluitnr, wi.volgnr_ind) AS INT) AS beschikte_voorziening_id,
    CAST(NULL AS VARCHAR(20)) AS code,
    CAST(wi.dd_eind AS DATE) AS datumeinde,
    CAST(NULL AS DATE) AS datumeindeoorspronkelijk,
    CAST(wi.dd_begin AS DATE) AS datumstart,
    CAST(NULL AS INT) AS eenheid_enum_id,
    CAST(NULL AS INT) AS frequentie_enum_id,
    CAST(NULL AS INT) AS heeft_leveringsvorm_293_id,
    CAST(NULL AS INT) AS is_voorziening_voorziening_id,
    CAST(NULL AS INT) AS leveringsvorm_287_enum_id,
    CAST(wi.volume AS INT) AS omvang,
    CAST(NULL AS DATE) AS redeneinde,
    wi.status_indicatie AS status,
    CAST(NULL AS INT) AS toegewezen_product_toewijzing_id,
    CAST(NULL AS INT) AS wet_enum_id
FROM stg.wvind_b wi
LEFT JOIN stg.szregel sr ON wi.kode_regeling = sr.kode_regeling
LEFT JOIN stg.wvbesl wb ON wi.besluitnr = wb.besluitnr
LEFT JOIN stg.wvdos wd ON wi.besluitnr = wd.besluitnr AND wi.volgnr_ind = wd.volgnr_ind
LEFT JOIN stg.abc_refcod ar ON (
    wd.kode_reden_einde_voorz = ar.code
    AND (
        (sr.omschryving = 'JEUGDWET' AND ar.domein = 'JZG_REDEN_EINDE_PRODUCT')
        OR (sr.omschryving != 'JEUGDWET' AND ar.domein = 'WVRTEIND')
    )
)
