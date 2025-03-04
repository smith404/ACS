CREATE SCHEMA IF NOT EXISTS data_language;

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
data_language.feed_attribute_values (
feed_attribute_id INTEGER NOT NULL
,feed_attribute_value VARCHAR(50) NOT NULL
,feed_attribute_value_label VARCHAR(100)
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL
,PRIMARY KEY (feed_attribute_id, feed_attribute_value));

CREATE SEQUENCE IF NOT EXISTS feed_mapper_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.feed_mapper (
map_id INTEGER DEFAULT nextval('feed_mapper_key') NOT NULL
,map_name VARCHAR(50) NOT NULL
,map_description VARCHAR(255)
,from_domain VARCHAR(10) NOT NULL
,to_domain VARCHAR(10) NOT NULL
,target_attr_value_id VARCHAR(50) NOT NULL
,PRIMARY KEY (map_id));

CREATE SEQUENCE IF NOT EXISTS feed_attribute_value_mapping_rule_key START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS
data_language.feed_attribute_value_mapping_rule (
rule_id INTEGER DEFAULT nextval('feed_attribute_value_mapping_rule_key') NOT NULL
,rule_name VARCHAR(50) NOT NULL
,rule_description VARCHAR(255)
,rule_type VARCHAR(50) NOT NULL -- ENUM('MAX', 'MIN', 'SUM', 'AVE', 'AND', 'OR', 'RENAME', 'REPLACE', 'REMOVE', 'ADD', 'COPY', 'MOVE', 'FILTER', 'SPLIT', 'MERGE', 'CONCATENATE', 'SPLIT', 'MERGE', 'CONCATENATE', 'LOOKUP', 'MAP')
,target_attr_value_id VARCHAR(50) NOT NULL
,valid_from DATE NOT NULL
,valid_to DATE NOT NULL
,PRIMARY KEY (rule_id));


CREATE TABLE IF NOT EXISTS
data_language.feed_attribute_value_mapping (
rule_id INTEGER NOT NULL
,step_number INTEGER NOT NULL
,comparison_operator VARCHAR(10) NOT NULL -- ENUM('=', '!=', '>', '<', '>=', '<=', 'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL')    
,feed_attribute_id INTEGER NOT NULL
,feed_attribute_value VARCHAR(50) NOT NULL
,PRIMARY KEY (rule_id, step_number));

