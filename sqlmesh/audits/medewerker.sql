-- Audits for silver.medewerker
-- Based on GGM DDL: MEDEWERKER table constraints

AUDIT (
    name assert_medewerker_pk_not_null,
    blocking false
);
SELECT * FROM silver.medewerker WHERE medewerker_id IS NULL;

AUDIT (
    name assert_medewerker_pk_unique,
    blocking false
);
SELECT medewerker_id, COUNT(*) as cnt
FROM silver.medewerker
GROUP BY medewerker_id
HAVING COUNT(*) > 1;

AUDIT (
    name assert_medewerker_functie_max_length,
    blocking false
);
SELECT * FROM silver.medewerker WHERE LENGTH(CAST(functie AS VARCHAR)) > 50;

AUDIT (
    name assert_medewerker_achternaam_max_length,
    blocking false
);
SELECT * FROM silver.medewerker WHERE LENGTH(CAST(achternaam AS VARCHAR)) > 200;

AUDIT (
    name assert_medewerker_telefoon_max_length,
    blocking false
);
SELECT * FROM silver.medewerker WHERE LENGTH(CAST(telefoonnummer AS VARCHAR)) > 20;
