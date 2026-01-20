-- Audits for silver.wet_enum
-- Based on GGM DDL: WET_ENUM table constraints

AUDIT (
    name assert_wet_enum_pk_not_null,
    blocking false
);
SELECT * FROM silver.wet_enum WHERE wet_enum_id IS NULL;

AUDIT (
    name assert_wet_enum_pk_unique,
    blocking false
);
SELECT wet_enum_id, COUNT(*) as cnt
FROM silver.wet_enum
GROUP BY wet_enum_id
HAVING COUNT(*) > 1;
