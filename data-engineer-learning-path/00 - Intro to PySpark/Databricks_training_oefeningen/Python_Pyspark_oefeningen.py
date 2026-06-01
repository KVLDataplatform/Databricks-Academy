# Databricks notebook source
# MAGIC %md
# MAGIC #Data Engineering Training: Van Bronze naar Silver
# MAGIC 
# MAGIC Het doel van dit notebook is om u vertrouwd te maken met PySpark en SQL binnen de Databricks omgeving. We doorlopen het proces van het inladen van ruwe data (Bronze), het uitvoeren van transformaties, en het gestructureerd opslaan van de verrijkte data (Silver).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 1: Basisbeginselen Python
# MAGIC 
# MAGIC ### Tutorial: Syntax en Variabelen
# MAGIC Voordat we dataframes manipuleren, behandelen we kort de basis van Python. In Python worden waarden opgeslagen in variabelen. 
# MAGIC * Toewijzing: `mijn_leeftijd = 30`
# MAGIC * Strings (tekst): `stad = "Rotterdam"`
# MAGIC * Strings samenvoegen: `volledige_naam = voornaam + " " + achternaam`
# MAGIC * Ingebouwde functies: `aantal_letters = len("Schiedam")`
# MAGIC * Lijsten (Lists): `steden = ["Rotterdam", "Vlaardingen", "Dordrecht"]`
# MAGIC 
# MAGIC ### Oefening: Python Basis
# MAGIC Voer de onderstaande basisstappen uit om de syntax eigen te maken.

# COMMAND ----------

# 1. Print de tekst 'Start pipeline'
print()

# 2. Wijs uw naam toe aan een variabele genaamd 'voornaam' en print het resultaat
voornaam = 
print(voornaam)

# 3. Wijs uw achternaam toe aan een variabele, combineer deze met de voornaam (inclusief spatie) en print het resultaat
achternaam = 

# 4. Gebruik de functie len() om de lengte van de volledige naam te berekenen. Print het resultaat.
lengte_naam = 

# 5. Splits de string "Databricks training DCMR" in afzonderlijke woorden via .split(" ") en sla het op als 'woorden_lijst'. Print het resultaat.
zin = "Databricks training DCMR"
woorden_lijst = 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 2: Verschillende Datatypes Inladen (Bronze)
# MAGIC 
# MAGIC ### Tutorial: Data Inladen in PySpark en SQL Views
# MAGIC Binnen Unity Catalog slaan we ruwe bestanden op in een Volume. In de bronzelaag staan drie bestanden met meldingen, elk in een ander formaat. PySpark vereist voor elk formaat specifieke inlees-opties:
# MAGIC 
# MAGIC * **CSV:** Vereist vaak het specificeren van een header en het afleiden van het schema.
# MAGIC   `spark.read.csv("pad", header=True, inferSchema=True)`
# MAGIC * **JSON:** Als JSON over meerdere regels is opgemaakt, is een extra optie nodig.
# MAGIC   `spark.read.json("pad", multiLine=True)`
# MAGIC * **Parquet:** Dit is een geoptimaliseerd dataformaat dat het schema al bevat. Dit leest het makkelijkst in.
# MAGIC   `spark.read.parquet("pad")`
# MAGIC 
# MAGIC **Integratie met SQL:** Om een PySpark DataFrame toegankelijk te maken voor SQL queries, maken we een tijdelijke view aan via:
# MAGIC `df.createOrReplaceTempView("tijdelijke_view_naam")`
# MAGIC 
# MAGIC ### Oefening: Bestanden Inlezen en SQL View Maken

# COMMAND ----------

# Pas '<catalog>' en '<schema>' aan naar de namen in de trainingsomgeving
base_path = "/Volumes/<catalog>/<schema>/bronze/"

# 1. Lees het CSV bestand in: "meldingen_deel_1.csv". Zet header en inferSchema op True.
pad_csv = base_path + "meldingen_deel_1.csv"
df_csv = 

# 2. Lees het JSON bestand in: "meldingen_deel_2.json". Zet de multiLine optie op True.
pad_json = base_path + "meldingen_deel_2.json"
df_json = 

# 3. Lees het Parquet bestand in: "meldingen_deel_3.parquet".
pad_parquet = base_path + "meldingen_deel_3.parquet"
df_parquet = 

# 4. Toon het schema van het Parquet dataframe om de datatypes te inspecteren.
# Vul code in:

# 5. We gaan voor de rest van de oefeningen verder met de CSV data. 
# Maak een tijdelijke SQL view aan van df_csv genaamd 'vw_bronze_meldingen'
df_csv.createOrReplaceTempView("vw_bronze_meldingen")

# COMMAND ----------

# MAGIC %md
# MAGIC Test uw ingeladen view door de onderstaande SQL cel uit te voeren:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_bronze_meldingen LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 3: Data Manipulatie en Selecties
# MAGIC 
# MAGIC ### Tutorial: Method Chaining (PySpark) vs Declaratief (SQL)
# MAGIC In PySpark gebruiken we method chaining om transformaties opeenvolgend uit te voeren:
# MAGIC * Selecteren: `.select("kolom1", "kolom2")`
# MAGIC * Filteren: `.filter(F.col("status") == "Open")`
# MAGIC * Hernoemen: `.withColumnRenamed("oud", "nieuw")` of via `.alias("nieuw")`
# MAGIC 
# MAGIC In SQL gebruiken we hiervoor de standaard `SELECT`, `WHERE`, en `AS` statements.
# MAGIC 
# MAGIC ### Oefening: Data Filteren en Hernoemen

# COMMAND ----------

import pyspark.sql.functions as F

# PySpark Oefening
# Gebruik df_csv. Filter de data (ernst is "Hoog" EN status is "Open").
# Selecteer melding_id, categorie en locatie (hernoem locatie naar 'gemeente'). Sla op in df_hoge_prioriteit.
df_hoge_prioriteit = (
    df_csv
    # Vul aan
)
display(df_hoge_prioriteit)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL Oefening
# MAGIC -- Schrijf exact dezelfde query als hierboven, maar dan in SQL, gebruikmakend van vw_bronze_meldingen.
# MAGIC SELECT 
# MAGIC   -- Vul kolommen en aliassen in
# MAGIC FROM vw_bronze_meldingen
# MAGIC WHERE -- Vul condities in

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 4: Datatypes Aanpassen en Logica Toevoegen
# MAGIC 
# MAGIC ### Tutorial: Casten en Voorwaardelijke Logica
# MAGIC Kolommen toevoegen of overschrijven in PySpark gebeurt met `.withColumn()`. 
# MAGIC * Datatype wijzigen: `.cast("date")`
# MAGIC * Voorwaardelijke logica: `F.when(conditie, actie).otherwise(alternatief)`
# MAGIC 
# MAGIC In SQL is het equivalent van `when.otherwise` de `CASE WHEN ... THEN ... ELSE ... END` expressie.
# MAGIC 
# MAGIC ### Oefening: Verrijken naar Silver Standaarden

# COMMAND ----------

# PySpark Oefening
# Gebruik df_csv. Voeg een nieuwe kolom "urgentie_vlag" toe. 
# Logica: Als ernst "Hoog" is, vul met "DIRECT ACTIE", anders "REGULIER".
df_verrijkt = (
    df_csv
    # Vul aan
)
display(df_verrijkt)
# Maak een view voor de volgende stap
df_verrijkt.createOrReplaceTempView("vw_silver_prep")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL Oefening
# MAGIC -- Pas dezelfde logica (urgentie_vlag) toe met een CASE WHEN statement. Gebruik vw_bronze_meldingen.
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   -- Schrijf hier de CASE WHEN statement
# MAGIC FROM vw_bronze_meldingen

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 5: Groeperen, Aggregeren en Joins
# MAGIC 
# MAGIC ### Tutorial: Berekeningen en Relaties
# MAGIC * Groeperen in PySpark: `.groupBy("categorie").agg(F.count("melding_id").alias("aantal"))`
# MAGIC * Joins in PySpark: `.join(andere_tabel, on="sleutel_kolom", how="left")`
# MAGIC 
# MAGIC ### Oefening: Analyse en Verrijking met Referentiedata

# COMMAND ----------

# Voorbereiding: Referentiedata (SLA) aanmaken
sla_data = [("Laag", 14), ("Gemiddeld", 7), ("Hoog", 2)]
df_sla = spark.createDataFrame(sla_data, ["ernst", "max_afhandel_dagen"])
df_sla.createOrReplaceTempView("vw_ref_sla")

# PySpark Oefening 1: Groeperen
# Groepeer df_csv op 'categorie', tel het aantal meldingen (alias: aantal_meldingen) en sorteer aflopend.
df_telling = (
    # Vul aan
)

# PySpark Oefening 2: Joins
# Koppel df_csv met df_sla op de kolom 'ernst' via een left join. Sla op in df_silver_finaal.
df_silver_finaal = (
    # Vul aan
)
display(df_silver_finaal)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL Oefening: Join
# MAGIC -- Voer een LEFT JOIN uit tussen vw_bronze_meldingen (alias b) en vw_ref_sla (alias s) op de kolom ernst.
# MAGIC SELECT 
# MAGIC   b.*,
# MAGIC   s.max_afhandel_dagen
# MAGIC FROM vw_bronze_meldingen b
# MAGIC -- Schrijf hier de JOIN clausule

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 6: Data Opslaan in de Silver Laag
# MAGIC 
# MAGIC ### Tutorial: Wegschrijven als Delta Tabel
# MAGIC Nu de data is opgeschoond en verrijkt (Silver niveau), slaan we deze persistent op in de Unity Catalog. We gebruiken het Delta formaat.
# MAGIC * PySpark: `df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.tabelnaam")`
# MAGIC * Partitionering kan worden toegevoegd met `.partitionBy("locatie")` om queries te optimaliseren.
# MAGIC 
# MAGIC ### Oefening: Schrijven en Partitioneren

# COMMAND ----------

# PySpark Oefening
# Schrijf df_silver_finaal weg als een Delta tabel in uw schema met de naam 'silver_milieu_meldingen'.
# Partitioneer de data op "locatie" en gebruik mode("overwrite").

# Vul commando in:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL Alternatief (ter informatie)
# MAGIC -- In SQL kunt u een nieuwe tabel creëren of overschrijven door het volgende
