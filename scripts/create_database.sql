CREATE SCHEMA IF NOT EXISTS data_language;

CREATE TABLE IF NOT EXISTS
data_language.feed_domain_mapings (
from_attribute_id INTEGER NOT NULL
,to_attribute_id INTEGER NOT NULL
,directional BOOLEAN DEFAULT true NOT NULL
,PRIMARY KEY (from_attribute_id, to_attribute_id));

CREATE SEQUENCE IF NOT EXISTS feed_attribute_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.feed_attributes (
feed_attribute_id INTEGER DEFAULT nextval('feed_attribute_key') NOT NULL
,domain VARCHAR(10) NOT NULL
,attribute_name VARCHAR(50) NOT NULL
,attr_description VARCHAR(255)
,external_location VARCHAR(255)
,PRIMARY KEY (feed_attribute_id));

CREATE TABLE IF NOT EXISTS
data_language.feed_attribute_value (
feed_attribute_id INTEGER NOT NULL
,feed_attribute_value VARCHAR(50) NOT NULL
,feed_attribute_value_labael VARCHAR(100)
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL
,PRIMARY KEY (feed_attribute_id, feed_attribute_value));

CREATE SEQUENCE IF NOT EXISTS feed_attribute_value_mapping_rule_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.feed_attribute_value_mapping_rule (
rule_id INTEGER DEFAULT nextval('feed_attribute_value_mapping_rule_key') NOT NULL
,from_domain VARCHAR(10) NOT NULL
,to_domain VARCHAR(10) NOT NULL
,rule_name VARCHAR(50) NOT NULL
,rule_description VARCHAR(255)
,target_attr_value_id VARCHAR(50) NOT NULL
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL
,PRIMARY KEY (rule_id));


CREATE TABLE IF NOT EXISTS
data_language.feed_attribute_value_mapping (
rule_id INTEGER NOT NULL
,step_number INTEGER NOT NULL
,feed_attribute_id INTEGER NOT NULL
,feed_attribute_value VARCHAR(50) NOT NULL
,PRIMARY KEY (rule_id, step_number));

