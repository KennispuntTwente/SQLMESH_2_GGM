-- Audits for silver.client
-- Based on GGM DDL: CLIENT table constraints

AUDIT (
    name assert_client_pk_not_null,
    blocking false
);
SELECT * FROM silver.client WHERE rechtspersoon_id IS NULL;

AUDIT (
    name assert_client_pk_unique,
    blocking false
);
SELECT rechtspersoon_id, COUNT(*) as cnt
FROM silver.client
GROUP BY rechtspersoon_id
HAVING COUNT(*) > 1;

AUDIT (
    name assert_client_code_max_length,
    blocking false
);
SELECT * FROM silver.client WHERE LENGTH(CAST(code AS VARCHAR)) > 80;
