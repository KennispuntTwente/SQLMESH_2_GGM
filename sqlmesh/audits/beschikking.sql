-- Audits for silver.beschikking
-- Based on GGM DDL: BESCHIKKING table constraints

AUDIT (
    name assert_beschikking_pk_not_null,
    blocking false
);
SELECT * FROM silver.beschikking WHERE beschikking_id IS NULL;

AUDIT (
    name assert_beschikking_pk_unique,
    blocking false
);
SELECT beschikking_id, COUNT(*) as cnt
FROM silver.beschikking
GROUP BY beschikking_id
HAVING COUNT(*) > 1;

AUDIT (
    name assert_beschikking_code_max_length,
    blocking false
);
SELECT * FROM silver.beschikking WHERE LENGTH(CAST(code AS VARCHAR)) > 20;

AUDIT (
    name assert_beschikking_commentaar_max_length,
    blocking false
);
SELECT * FROM silver.beschikking WHERE LENGTH(CAST(commentaar AS VARCHAR)) > 200;

AUDIT (
    name assert_fk_beschikking_client,
    blocking false
);
SELECT b.*
FROM silver.beschikking b
LEFT JOIN silver.client c ON b.client_id = c.rechtspersoon_id
WHERE b.client_id IS NOT NULL AND c.rechtspersoon_id IS NULL;

AUDIT (
    name assert_fk_beschikking_beschikte_voorziening,
    blocking false
);
SELECT b.*
FROM silver.beschikking b
LEFT JOIN silver.beschikte_voorziening bv ON b.heeft_voorzieningen_beschikte_voorziening_id = bv.beschikte_voorziening_id
WHERE b.heeft_voorzieningen_beschikte_voorziening_id IS NOT NULL AND bv.beschikte_voorziening_id IS NULL;
