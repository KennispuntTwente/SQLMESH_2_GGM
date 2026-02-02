# cssd

Deze map bevat GGM DDL voor de use-case van gemeente Oldenzaal & gemeente Rijssen-Holten,
waarbij enkele tabellen worden ontsloten uit Centric Suite 4 Sociaal Domein (CSSD).

Ten opzichte van de originele DDL zijn de volgende wijzigingen doorgevoerd:
- Tabellen die we niet gebruiken, zijn verwijderd uit de DDL.
- Diverse constraints die het laden van data in de weg staan, zijn verwijderd uit de DDL. (Constraints worden mogelijk in de toekomst omgezet
naar audits/tests.)

Deze DDL correspondeert aan de tabellen die gemodelleerd worden in 
'transform/models/silver'. 

In 'scripts/validate_schema.py' verifiëren we of de tabellen die gemodelleerd worden in 'transform/models/silver' overeenkomen met de tabellen die in de SQL-code in 'ggm/selectie/cssd' gedefinieerd zijn (validatie wordt uitgevoerd in GitHub Actions).

