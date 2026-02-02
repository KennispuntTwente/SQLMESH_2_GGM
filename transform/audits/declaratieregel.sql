-- Audits for silver.declaratieregel
-- Based on GGM DDL: DECLARATIEREGEL table constraints

AUDIT (
    name assert_declaratieregel_pk_not_null,
    blocking false
);
SELECT * FROM silver.declaratieregel WHERE declaratieregel_id IS NULL;

AUDIT (
    name assert_declaratieregel_pk_unique,
    blocking false
);
SELECT declaratieregel_id, COUNT(*) as cnt
FROM silver.declaratieregel
GROUP BY declaratieregel_id
HAVING COUNT(*) > 1;

AUDIT (
    name assert_fk_declaratieregel_client,
    blocking false
);
SELECT d.*
FROM silver.declaratieregel d
LEFT JOIN silver.client c ON d.betreft_client_id = c.rechtspersoon_id
WHERE d.betreft_client_id IS NOT NULL AND c.rechtspersoon_id IS NULL;

AUDIT (
    name assert_fk_declaratieregel_beschikking,
    blocking false
);
SELECT d.*
FROM silver.declaratieregel d
LEFT JOIN silver.beschikking b ON d.is_voor_beschikking_id = b.beschikking_id
WHERE d.is_voor_beschikking_id IS NOT NULL AND b.beschikking_id IS NULL;
