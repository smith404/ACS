CREATE SCHEMA IF NOT EXISTS data_language;

CREATE SEQUENCE IF NOT EXISTS external_attribute_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.external_attribute (
external_attribute_id INTEGER DEFAULT nextval('external_attribute_key') PRIMARY KEY NOT NULL
,external_attribute_value VARCHAR(50) NOT NULL
,external_data_id INTEGER NOT NULL
,external_attr_label VARCHAR(50) NOT NULL
,external_table_name VARCHAR(50) NOT NULL);

CREATE TABLE IF NOT EXISTS
data_language.external_attribute_value (
external_attribute_id INTEGER NOT NULL
,external_attribute_value VARCHAR(50) NOT NULL
,external_attribute_value_labael VARCHAR(50)
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL
,PRIMARY KEY (external_attribute_id, external_attribute_value));

CREATE TABLE IF NOT EXISTS
data_language.host_attribute_value_mapping (
external_attribute_id INTEGER NOT NULL
,external_attribute_2_id INTEGER NOT NULL
,external_attribute_value VARCHAR(50) NOT NULL
,external_attribute_2_value VARCHAR(50) NOT NULL
,spire_attr_value_id VARCHAR(50) NOT NULL
,mapping_direction_id INTEGER NOT NULL
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL
,PRIMARY KEY (external_attribute_id, external_attribute_2_id));

CREATE SEQUENCE IF NOT EXISTS spire_attribute_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.spire_attribute (
spire_attribute_id INTEGER DEFAULT nextval('spire_attribute_key') PRIMARY KEY NOT NULL
,spire_attribute_name VARCHAR(50) NOT NULL
,spire_attribute_label VARCHAR(150) NOT NULL
,master_data_id INTEGER
,master_data_consumer_id INTEGER
,description VARCHAR(255)
,comments VARCHAR(255)
,transalte_to_mdm INTEGER NOT NULL
,master_data_table_name VARCHAR(50));

CREATE SEQUENCE IF NOT EXISTS spire_attribute_value_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.spire_attribute_value (
spire_attribute_value_id INTEGER DEFAULT nextval('spire_attribute_value_key') PRIMARY KEY NOT NULL
,spire_attribute_id INTEGER NOT NULL
,spire_attribute_value VARCHAR(50) NOT NULL
,sdl_id INTEGER
,spire_attribute_value_label VARCHAR(50) NOT NULL
,parent_attribute_value_id INTEGER
,is_default_value INTEGER NOT NULL
,spire_attribute_value_label_short VARCHAR(50)
,level_no INTEGER
,is_active INTEGER NOT NULL
,is_valid INTEGER NOT NULL
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL);

