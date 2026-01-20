-- Audits for silver.beschikte_voorziening
-- Based on GGM DDL: BESCHIKTE_VOORZIENING table constraints

AUDIT (
    name assert_beschikte_voorziening_pk_not_null,
    blocking false
);
SELECT * FROM silver.beschikte_voorziening WHERE beschikte_voorziening_id IS NULL;

AUDIT (
    name assert_beschikte_voorziening_pk_unique,
    blocking false
);
SELECT beschikte_voorziening_id, COUNT(*) as cnt
FROM silver.beschikte_voorziening
GROUP BY beschikte_voorziening_id
HAVING COUNT(*) > 1;

AUDIT (
    name assert_beschikte_voorziening_status_max_length,
    blocking false
);
SELECT * FROM silver.beschikte_voorziening WHERE LENGTH(CAST(status AS VARCHAR)) > 50;

AUDIT (
    name assert_fk_beschikte_voorziening_wet_enum,
    blocking false
);
SELECT bv.*
FROM silver.beschikte_voorziening bv
LEFT JOIN silver.wet_enum we ON bv.wet_enum_id = we.wet_enum_id
WHERE bv.wet_enum_id IS NOT NULL AND we.wet_enum_id IS NULL;
