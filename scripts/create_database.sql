CREATE SCHEMA IF NOT EXISTS data_language;

CREATE SEQUENCE IF NOT EXISTS domain_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.domains (
    domain_id INTEGER DEFAULT nextval('domain_key') NOT NULL,
    domain VARCHAR(10) NOT NULL UNIQUE,
    domain_description VARCHAR(255),
    PRIMARY KEY (domain_id)
);

CREATE SEQUENCE IF NOT EXISTS data_assets_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.data_assets (
    data_asset_id INTEGER DEFAULT nextval('data_assets_key') NOT NULL,
    domain VARCHAR(10) NOT NULL,
    asset_name VARCHAR(50) NOT NULL UNIQUE,
    asset_description VARCHAR(255),
    medallion_layer VARCHAR(10) NOT NULL,
    container VARCHAR(255) NOT NULL,
    feed_path VARCHAR(255) NOT NULL,
    PRIMARY KEY (data_asset_id)
);

CREATE SEQUENCE IF NOT EXISTS data_attribute_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.data_attributes (
    data_attribute_id INTEGER DEFAULT nextval('data_attribute_key') NOT NULL,
    attribute_name VARCHAR(50) NOT NULL,
    attribute_description VARCHAR(255),
    attribute_type VARCHAR(10) NOT NULL, -- ENUM('STRING', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'DATETIME', 'BOOLEAN', 'ENUM', 'OBJECT', 'ARRAY', 'MAP')
    attribute_length INTEGER NOT NULL,
    external_source VARCHAR(255),
    PRIMARY KEY (data_attribute_id)
);

CREATE TABLE IF NOT EXISTS data_language.data_asset_attributes (
    data_asset_id INTEGER NOT NULL,
    data_attribute_id INTEGER NOT NULL,
    mandatory BOOLEAN NOT NULL,
    is_key BOOLEAN NOT NULL,
    default_value VARCHAR(255),
    format_string VARCHAR(255),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (data_asset_id, data_attribute_id),
    FOREIGN KEY (data_asset_id) REFERENCES data_language.data_assets(data_asset_id),
    FOREIGN KEY (data_attribute_id) REFERENCES data_language.data_attributes(data_attribute_id)
);

CREATE TABLE IF NOT EXISTS data_language.attribute_values (
    data_attribute_id INTEGER NOT NULL,
    attribute_value VARCHAR(100) NOT NULL,
    attribute_value_label VARCHAR(255),
    is_default BOOLEAN NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (data_attribute_id, attribute_value),
    FOREIGN KEY (data_attribute_id) REFERENCES data_language.data_attributes(data_attribute_id)
);

CREATE SEQUENCE IF NOT EXISTS mappings_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.mappings (
    map_id INTEGER DEFAULT nextval('mappings_key') NOT NULL,
    map_name VARCHAR(50) NOT NULL UNIQUE,
    map_description VARCHAR(255),
    from_asset_id INTEGER NOT NULL,
    to_asset_id INTEGER NOT NULL,
    target_attr_value_id VARCHAR(50) NOT NULL,
    PRIMARY KEY (map_id),
    FOREIGN KEY (from_asset_id) REFERENCES data_language.data_assets(data_asset_id),
    FOREIGN KEY (to_asset_id) REFERENCES data_language.data_assets(data_asset_id)
);

CREATE SEQUENCE IF NOT EXISTS mapping_rule_group_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.mapping_rule_groups (
    rule_group_id INTEGER DEFAULT nextval('mapping_rule_group_key') NOT NULL,
    group_name VARCHAR(50) NOT NULL,
    group_name_description VARCHAR(255),
    target_attribute_id INTEGER NOT NULL,
    target_attribute_value VARCHAR(255) NOT NULL,
    PRIMARY KEY (rule_group_id),
    FOREIGN KEY (target_attribute_id) REFERENCES data_language.data_attributes(data_attribute_id)
);

CREATE SEQUENCE IF NOT EXISTS mapping_rule_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.mapping_rules (
    rule_id INTEGER DEFAULT nextval('mapping_rule_key') NOT NULL,
    rule_group_id INTEGER NOT NULL,
    step_number INTEGER NOT NULL,
    rule_name VARCHAR(50) NOT NULL,
    rule_type VARCHAR(50) NOT NULL, -- ENUM('MAX', 'MIN', 'SUM', 'AVE', 'AND', 'OR', 'RENAME', 'REPLACE', 'REMOVE', 'ADD', 'COPY', 'MOVE', 'FILTER', 'SPLIT', 'MERGE', 'CONCATENATE', 'LOOKUP', 'MAP')
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (rule_id),
    FOREIGN KEY (rule_group_id) REFERENCES data_language.mapping_rule_groups(rule_group_id)
);

CREATE TABLE IF NOT EXISTS data_language.mapping_rule_parts (
    rule_id INTEGER NOT NULL,
    step_number INTEGER NOT NULL,
    comparison_operator VARCHAR(10) NOT NULL, -- ENUM('=', '!=', '>', '<', '>=', '<=', 'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL')    
    feed_attribute_id INTEGER NOT NULL,
    feed_attribute_value VARCHAR(50) NOT NULL,
    PRIMARY KEY (rule_id, step_number),
    FOREIGN KEY (feed_attribute_id) REFERENCES data_language.data_attributes(data_attribute_id),
    FOREIGN KEY (rule_id) REFERENCES data_language.mapping_rules(rule_id)
);

CREATE TABLE IF NOT EXISTS data_language.mapping_rule_assignments (
    map_id INTEGER NOT NULL,
    rule_group_id INTEGER NOT NULL,
    step_number INTEGER NOT NULL,
    PRIMARY KEY (map_id, rule_group_id),
    FOREIGN KEY (map_id) REFERENCES data_language.mappings(map_id),
    FOREIGN KEY (rule_group_id) REFERENCES data_language.mapping_rule_groups(rule_group_id)
);

