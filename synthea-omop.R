# End to end description of generating synthetic data with synthea and ETL into
# the OMOP CDM.

################################################################################
# Generate the synthetic data with synthea.
################################################################################
# https://github.com/synthetichealth/synthea
# The master branch of synthea is a bit ahead of the R library here, so stick
# with v3.0.0 (https://github.com/synthetichealth/synthea/releases/tag/v3.0.0)
# get the synthea-with-dependancies.jar file.
# The output needs to be saved as a CSV file, so create a file called
# synthea.properties with a line in it like:
# exporter.csv.export = true
# then run the code like:
# java -jar synthea-with-dependencies.jar -c synthea.properties
# This will generate data for ONE person, to make it 1000 try:
# java -jar synthea-with-dependencies.jar -p 1000 -c synthea.properties
# The same seed will generate identical populations, so for different populations:
# java -jar synthea-with-dependencies.jar -s 1 -p 1000 -c synthea.properties
# java -jar synthea-with-dependencies.jar -s 2 -p 1000 -c synthea.properties
# The codes used are SNOMED codes.

################################################################################
# Vocab mapping
################################################################################
# The SNOMED codes in the synthea data need to be mapped, so a vocab file is 
# needed. # Need to download vocabs from
# https://athena.ohdsi.org/vocabulary/list
# I *think* it just needs to be the SNOMED vocab. Unzip it.

################################################################################
# postgres DB set up
################################################################################
# The data goes into a postgres DB, so let's grab a docker image of one and
# set it up.

# docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres
# psql -h localhost -p 5432 -U postgres
# CREATE DATABASE synthea10;
# \connect synthea10;
# CREATE SCHEMA cdm_synthea10;
# CREATE SCHEMA native;

# Once we've imported some data we might want to run commands in postgres like:
# \c synthea10
# <-- connects to DB
# \dt native.*
# <-- look at the native schema, where the raw data is.
# \dt cdm_synthea10.*
# <-- look at the cdm schema, where the data will go.
# SELECT * FROM cdm_synthea10.condition_era;
# <-- look at the data

################################################################################
# R library set up
################################################################################
# We are going to use the ETL-Synthea library hosted on github:
# https://github.com/OHDSI/ETL-Synthea

devtools::install_github("OHDSI/ETL-Synthea", ref = "v2.0.0")

# May need to download JDBC drivers.
library(DatabaseConnector)
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "~/temp/jdbcDrivers")
downloadJdbcDrivers("postgresql")

library(ETLSyntheaBuilder)

cd <- DatabaseConnector::createConnectionDetails(
  dbms     = "postgresql", 
  server   = "localhost/synthea10", 
  user     = "postgres", 
  password = "mysecretpassword", 
  port     = 5432, 
  pathToDriver = "~/temp/jdbcDrivers"  
)

cdmSchema      <- "cdm_synthea10"
cdmVersion     <- "5.4"
syntheaVersion <- "3.0.0"
syntheaSchema  <- "native"
syntheaFileLoc <- "~/synthea/v3.0.0/output/csv/2024_03_08T11_48_01Z"
vocabFileLoc   <- "~/Downloads/vocab/vocabulary_download_v5_{fb150751-4c29-4e66-b452-db55849ef077}_1708370610702"

# Create the CDM tables in the postgres DB
ETLSyntheaBuilder::CreateCDMTables(connectionDetails = cd, cdmSchema = cdmSchema, cdmVersion = cdmVersion)

#Create the tables for the Synthea data to be loaded into
ETLSyntheaBuilder::CreateSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaVersion = syntheaVersion)

# Load the Synthea data in
ETLSyntheaBuilder::LoadSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaFileLoc = syntheaFileLoc)

# Load the vocab downloaded from Athena
ETLSyntheaBuilder::LoadVocabFromCsv(connectionDetails = cd, cdmSchema = cdmSchema, vocabFileLoc = vocabFileLoc)

# NEW STEP! This is where the state_map and all_visits tables are made!
ETLSyntheaBuilder::CreateMapAndRollupTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)

# ETL the data from the imported synthea tables into the CDM tables
# This will take hours to run with 1000 patients and a vanilla docker postgres image.
ETLSyntheaBuilder::LoadEventTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)
#ETLSyntheaBuilder::DropEventTables(connectionDetails = cd, cdmSchema = cdmSchema)
