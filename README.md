# SQLMESH_2_GGM

Dit is een open-source datapijplijn om gegevens te extraheren, te laden, en te transformeren naar het '[Gemeentelijk Gegevensmodel (GGM)](https://github.com/Gemeente-Delft/Gemeentelijk-Gegevensmodel)'. Gemaakt met de tools '[dlt](https://github.com/dlt-hub/dlt)' en '[SQLMesh](https://github.com/TobikoData/sqlmesh)'.

'dlt' is een open-source Python-framework voor het extraheren & laden van gegevens. 'SQLMesh' is een open-source framework voor data-modellering en -transformaties in Python & SQL. De tools ondersteunen diverse data-bronnen en -bestemmingen.
Ook als gemeenten een heel verschillend IT-landschap hebben, kunnen ze met deze tools toch samenwerken aan het volledige ELT-proces om gegevens onder te brengen in het GGM. Daarnaast ondersteunen de tools diverse best practices in moderne data engineering, zoals versiebeheer van datamodellen, geautomatiseerde tests en checks op datakwaliteit (zie de documentatie van '[dlt](https://dlthub.com/docs/)' en '[SQLMesh](https://sqlmesh.readthedocs.io/en/latest/)' voor meer informatie).

In deze repository worden 'dlt' en 'SQLMesh' gebruikt voor een data-pijplijn die gegevens uit de Oracle-database
van de Centric-applicatie 'CSSD' ('Centric Suite 4 Sociaal Domein') haalt en deze transformeert naar het GGM.
Daarbij kan het GGM draaien op diverse databases/datawarehouses (zoals PostgreSQL, MSSQL, Azure/Fabric, Snowflake, Databricks, BigQuery, et cetera). 

> Dit project is een versimpelde en verbeterde versie van '[SQL_2_GGM](https://github.com/KennispuntTwente/SQL_2_GGM)'
(zie: '[Meer informatie](#meer-informatie)').

## Pijplijn-overzicht

### 1 - Extract & load

'dlt' verbindt met de gegevensbron (in dit geval de Oracle-database van CSSD), extraheert de gegevens uit deze
bron en laadt deze in de doeldatabase naar een 'raw'-laag. 

Dit gebeurt in een append-modus waarbij een historisch overzicht van de gegevens wordt bewaard.
Elke run wordt de gehele tabel uit de bron geëxtraheerd en toegevoegd aan de 'raw'-laag,
met een unieke `_dlt_load_id` per run. Dit is de meest simpele load-strategie waarbij historie bewaard wordt; 
afhankelijk van de behoefte van de organisatie kan een andere load-strategie gekozen worden 
(zie: [documentatie van 'dlt'](https://dlthub.com/docs/general-usage/incremental-loading)).

> Zie: 'dlt/pipeline.py'.

### 2 - Transform

#### 2.1 - raw -> stg

'SQLMesh' neemt de 'raw'-gegevens en transformeert deze naar een 'stg'-laag (staging/brons),
waarin de meest recent geladen gegevens worden bewaard. Dit wordt gedaan door te filteren op de hoogste `_dlt_load_id`.
> Zie: 'sqlmesh/models/stg/'.

#### 2.2 - stg -> silver (GGM)

Vervolgens transformeert 'SQLMesh' deze 'stg'-gegevens naar een 'silver'-laag op basis van het GGM,
gemaakt naar de DDL-definities in 'ggm/selectie/cssd/*.sql' (zoals deze door de gemeente Delft zijn opgesteld).
Hiermee staat de data in het GGM.
> Zie: 'sqlmesh/models/silver/'.

##### Constraints in silver-laag

In de 'silver'-laag hanteren we enkel de vorm van de tabellen in het GGM
(d.w.z., kolomnamen en gegevenstypen). De constraints uit het GGM worden niet afgedwongen, 
zodat deze laag flexibel blijft en het laden van gegevens niet blokkeert.
Dit is ook hoe de gemeente Delft het GGM toepast in hun datawarehouse. Omdat de constraints wel relevante
informatie bieden over datakwaliteit, hebben we deze vertaald naar (non-blocking) '[audits](https://sqlmesh.readthedocs.io/en/latest/concepts/audits/)'. Met deze audits kan 'SQLMesh' rapporteren over mogelijke datakwaliteitsproblemen in de 'silver'-laag. 
We raden aan om die problemen voor zover mogelijk op te lossen in bronsystemen of om deze in een 'gold'-laag op te lossen.
> Zie: 'sqlmesh/audits/'.

##### GGM DDL -> SQLMesh-modellen

Om te te zorgen dat de 'silver'-modellen overeenkomen met de GGM DDL-definities,
zijn er enkele Python-scripts gemaakt die dit automatiseren en controleren.

In 'scripts/ddl_to_sqlmesh.py' is een converter opgenomen die GGM DDL-bestanden automatisch
converteert naar SQLMesh-modelbestanden. Dit script bevat een functie die de GGM DDL-bestanden
van Delft parset en hiermee 'SQLMesh'-modellen genereert met de juiste kolomnamen, gegevenstypen,
en SQLMesh-eigenschappen (zoals primary keys, grains, beschrijvingen, et cetera). Nadat
deze modellen zijn gegenereerd, kan je ze invullen met de juiste transformaties vanuit de 'stg'-laag.
Je kan dit script als volgt gebruiken:

```bash
# Dry-run: toon gegenereerde modellen zonder te schrijven
uv run python scripts/ddl_to_sqlmesh.py --ddl-dir ggm/selectie/cssd --dry-run

# Modellen genereren naar output-map
uv run python scripts/ddl_to_sqlmesh.py --ddl-dir ggm/selectie/cssd --output-dir sqlmesh/models/silver

# Specifieke tabellen converteren
uv run python scripts/ddl_to_sqlmesh.py --ddl-dir ggm/selectie/cssd --output-dir sqlmesh/models/silver --tables beschikking,client
```

In 'scripts/validate_schema.py' is een validatie-script opgenomen dat de met 'SQLMesh' opgestelde 'silver'-modellen vergelijkt met de DDL-definities van het GGM in 'ggm/selectie/cssd/*.sql'. Hierbij wordt
gecheckt op overeenstemming in kolomnamen en gegevenstypen. Dit script runt bij elke wijziging 
van de code in deze repository via GitHub Actions (zie: '.github/workflows/schema-validation.yml').

In 'scripts/validate_data.py' is een script opgenomen dat de daadwerkelijke geproduceerde data nogmaals valideert tegen de GGM DDL-definities. Voor deze pijplijn wordt dit in GitHub Actions uitgevoerd na het voltooien van de volledige pijplijn met synthetische gegevens tegen verschillende bestemmingen (PostgreSQL, MSSQL, etc.) (zie: '.github/workflows/pipeline.yml'; stap: 'Validate database schema against DDL').

## Gebruik 

### Installatie

Clone of download deze repository, open de betreffende map en run `uv sync`.
Met een 'bash'-terminal kan dat zo:

```bash
git clone https://github.com/KennispuntTwente/SQLMESH_2_GGM.git
cd SQLMESH_2_GGM
uv sync
```

> Dit project gebruikt '[uv](https://docs.astral.sh/uv/)' voor Python-installatie & Python-packages.
Installeer dus eerst 'uv' volgens de [instructies](https://docs.astral.sh/uv/getting-started/installation/).

> Bij gebruik van 'uv' wordt met `uv sync` automatisch een virtuele omgeving aangemaakt in de map '.venv/'.
Als je Python-code in deze repository wil uitvoeren, zorg er dan voor dat je deze virtuele omgeving activeert
(bijv., met `source .venv/bin/activate` op Linux/MacOS of `.venv\Scripts\activate` op Windows; of
door in je IDE de Python-interpreter in '.venv/' te selecteren). Als je `uv run ...` gebruikt, wordt de virtuele
omgeving automatisch geactiveerd bij het uitvoeren van de commando's.

### Configuratie

Om de pijplijn uit te kunnen voeren, moet je de bron- en bestemmingsdatabase configureren.

De **bron** voor deze specifieke pijplijn is dus de Oracle-database van de Centric-applicatie Suite 4 Sociaal Domein (CSSD). (Voor andere pijplijnen kunnen dit diverse andere databases en API's zijn die 'dlt' kan ontsluiten.) 

De **bestemming** kan elke database zijn waarop jouw gemeente het GGM wil toepassen, gegeven dat deze
ondersteund wordt door zowel 'dlt' als 'SQLMesh'. Dit kunnen diverse databases/datawarehouses zijn,
zowel on-premises als in de cloud, bijvoorbeeld PostgreSQL, MSSQL, Snowflake, Databricks, BigQuery, et cetera. Zie de [door 'dlt' ondersteunde databases](https://dlthub.com/docs/dlt-ecosystem/destinations) en 
de [door 'SQLMesh' ondersteunde databases](https://sqlmesh.readthedocs.io/en/stable/integrations/overview/).

De relevante credentials kan je instellen via environment-variabelen op je systeem,
of via een '.env'-bestand in de hoofdmap van dit project. Zie als voorbeeld het
bestand '.env.example'. Hierin staan de relevante variabelen voor de bron-database 
en diverse bestemmingsdatabases. Vul in wat je nodig hebt en sla het bestand op als '.env'.

Als alternatief en/of voor meer geavanceerde configuratie, kan je ook 'dlt/.dlt/config.toml' en
'dlt/.dlt/secrets.toml' gebruiken voor de 'dlt'-configuratie, en 'sqlmesh/config.yaml' voor de 'SQLMesh'-configuratie.

(Let op: in deze pijplijn zijn diverse bestemmingen van 'dlt' en 'SQLMesh' opgenomen,
maar niet alle. Om een nieuwe bestemming toe te voegen, voeg deze toe aan 'dlt/constants.py' en 
zorg dat de benodigde configuratie in 'sqlmesh/config.yaml' (SQLMesh) en/of '.env'/'dlt/.dlt/secrets.toml' ('dlt') is ingesteld.
Mogelijk moet je ook extra Python-packages installeren voor de nieuwe bestemming; 
gebruik hiervoor `uv add <package>` en daarna `uv sync`.)

### Uitvoeren

Je kan de pijplijn op diverse manieren uitvoeren, afhankelijk van je voorkeur en situatie.
Bijvoorbeeld via een lokale Python-omgeving met 'uv', of via Docker. Je kan de pijplijn 
ook in een ontwikkelomgeving met synthetische data uitvoeren voor test- en ontwikkeldoeleinden.

#### 1) Pijplijn uitvoeren in lokale Python-omgeving

Nu de configuratie gereed is, kun je de pijplijn uitvoeren. Je moet hierbij 
het type van jouw GGM-database-bestemming opgeven met `--dest`:

```bash
# Volledige pijplijn naar PostgreSQL
uv run pipeline --dest postgres

# Volledige pijplijn naar Snowflake
uv run pipeline --dest snowflake

# Volledige pijplijn naar MSSQL
uv run pipeline --dest mssql

# etc.
```

Gebruik `--help` om alle opties te zien:

```bash
uv run pipeline --help
```

> Zie: 'scripts/pipeline.py'.

#### 2) Pijplijn uitvoeren via Docker

Als alternatief voor het runnen met een lokale Python-omgeving/uv, kun je de pijplijn ook
uitvoeren via Docker. Dit is handig als je geen Python wilt installeren of als je de pijplijn
in een geïsoleerde, containerized omgeving wilt draaien.

Bouw eerst de Docker-image:

```bash
docker build -t ggm-pipeline .
```

Vervolgens kun je de pijplijn uitvoeren met de Docker-container:

```bash
# Bekijk alle opties
docker run --rm ggm-pipeline --help

# Met een .env bestand (aanbevolen):
docker run --rm --env-file .env ggm-pipeline --dest postgres

# Of met losse environment-variabelen:
docker run --rm \
  -e DESTINATION__POSTGRES__CREDENTIALS__HOST=mijn-db-server \
  -e DESTINATION__POSTGRES__CREDENTIALS__PORT=5432 \
  -e DESTINATION__POSTGRES__CREDENTIALS__DATABASE=ggm_dev \
  -e DESTINATION__POSTGRES__CREDENTIALS__USERNAME=ggm \
  -e DESTINATION__POSTGRES__CREDENTIALS__PASSWORD=wachtwoord \
  ggm-pipeline --dest postgres
```

#### 3) Uitvoeren met synthetische data

Voor ontwikkeling kan je met Docker een ontwikkelomgeving opzetten waarin je direct diverse
databases met synthetische data hebt draaien. Gebruik hiervoor:

```bash
# Start databases + laad synthetische data + voer pijplijn uit
uv run dev --dest postgres

# Of met MSSQL
uv run dev --dest mssql
```

> Dit vereist een installatie van [Docker](https://www.docker.com/) of [Podman](https://podman.io/) op je systeem,
welke draaiende moet zijn voordat je dit commando runt.

> Zie: 'scripts/dev.py', 'docker/' en 'synthetic/'.

## Meer informatie 

Dit project is ontstaan uit een samenwerking tussen Kennispunt Twente, de gemeente Oldenzaal,
en de gemeente Rijssen-Holten. Doel was om samen te werken op aan de implementatie van het GGM,
en daarbij ondanks verschillende IT-landschappen toch een gemeenschappelijke pijplijn te ontwikkelen.

Initieel zijn hiervoor Python-modules ontwikkeld in de '[SQL_2_GGM](https://github.com/KennispuntTwente/SQL_2_GGM)' repository.
Dat project functioneerde maar leunde op veel custom code. Door in dit project van 'dlt' en 'SQLMesh' gebruik te maken,
is de codebase veel compacter, beter te onderhouden, en makkelijker uitbreidbaar naar andere bronnen en bestemmingen,
alsmede beter te integreren in moderne data engineering workflows en best practices.

### Contact

Heb je vragen over dit project? Loop je tegen problemen aan? Of wil je samenwerken?
Neem contact op! De volgende organisaties & personen zijn betrokken bij dit project: 

**Kennispunt Twente**: 
- Luka Koning (l.koning@kennispunttwente.nl)
- Jos Quist (j.quist@kennispunttwente.nl)
- Hüseyin Seker (h.seker@kennispunttwente.nl)

**Gemeente Rijssen-Holten**: 
- Fabian Klaster (f.klaster@rijssen-holten.nl)
- Rien ten Hove (r.tenhove@rijssen-holten.nl)
- Joop Voortman (j.voortman@rijssen-holten.nl)

**Gemeente Oldenzaal**: 
- Joost Barink (j.barink@oldenzaal.nl)
- Odylia Luttikhuis (o.luttikhuis@oldenzaal.nl)

---

Voor technische vragen: neem contact op met Luka Koning en Joost Barink. Je mag ons mailen, maar kan ook een issue openen in de [GitHub-repository](https://github.com/KennispuntTwente/SQL_2_GGM/issues).

Voor vragen inzake (gemeentelijke) samenwerking: neem contact op met Jos Quist, Fabian Klaster en Joost Barink. Hiervoor graag per mail contact opnemen.
