CREATE SCHEMA IF NOT EXISTS data_language;

CREATE SEQUENCE IF NOT EXISTS data_asset_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.data_asset (
    data_asset_id INTEGER DEFAULT nextval('data_asset_key') NOT NULL,
    domain VARCHAR(10) NOT NULL,
    asset_name VARCHAR(50) NOT NULL,
    asset_description VARCHAR(255),
    medallion_layer VARCHAR(10) NOT NULL,
    container VARCHAR(255),
    feed_path VARCHAR(255),
    PRIMARY KEY (data_asset_id)
);

CREATE SEQUENCE IF NOT EXISTS data_attribute_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.data_attributes (
    data_attribute_id INTEGER DEFAULT nextval('data_attribute_key') NOT NULL,
    attribute_name VARCHAR(50) NOT NULL,
    attribute_description VARCHAR(255),
    external_source VARCHAR(255),
    PRIMARY KEY (data_attribute_id)
);

CREATE TABLE IF NOT EXISTS data_language.data_asset_attributes (
    data_asset_id INTEGER NOT NULL,
    data_attribute_id INTEGER NOT NULL,
    mandatory BOOLEAN NOT NULL,
    is_key BOOLEAN NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (data_asset_id, data_attribute_id),
    FOREIGN KEY (data_asset_id) REFERENCES data_language.data_asset(data_asset_id),
    FOREIGN KEY (data_attribute_id) REFERENCES data_language.data_attributes(data_attribute_id)
);

CREATE TABLE IF NOT EXISTS data_language.attribute_values (
    data_attribute_id INTEGER NOT NULL,
    feed_attribute_value VARCHAR(50) NOT NULL,
    feed_attribute_value_label VARCHAR(100),
    is_default BOOLEAN NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (data_attribute_id, attribute_values),
    FOREIGN KEY (data_attribute_id) REFERENCES data_language.data_attributes(data_attribute_id)
);

CREATE SEQUENCE IF NOT EXISTS feed_mapper_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.transform_mapper (
    map_id INTEGER DEFAULT nextval('feed_mapper_key') NOT NULL,
    map_name VARCHAR(50) NOT NULL,
    map_description VARCHAR(255),
    from_domain VARCHAR(10) NOT NULL,
    to_domain VARCHAR(10) NOT NULL,
    target_attr_value_id VARCHAR(50) NOT NULL,
    PRIMARY KEY (map_id)
);

CREATE TABLE IF NOT EXISTS data_language.transform_mapper_rules (
    map_id INTEGER NOT NULL,
    rule_id INTEGER NOT NULL,
    PRIMARY KEY (map_id, rule_id),
    FOREIGN KEY (map_id) REFERENCES data_language.transform_mapper(map_id),
    FOREIGN KEY (rule_id) REFERENCES data_language.feed_attribute_mapping_rules(rule_id)
);

CREATE SEQUENCE IF NOT EXISTS feed_attribute_mapping_rule_key 
    START WITH 1 
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS data_language.feed_attribute_mapping_rules (
    rule_id INTEGER DEFAULT nextval('feed_attribute_mapping_rule_key') NOT NULL,
    rule_name VARCHAR(50) NOT NULL,
    rule_description VARCHAR(255),
    rule_type VARCHAR(50) NOT NULL, -- ENUM('MAX', 'MIN', 'SUM', 'AVE', 'AND', 'OR', 'RENAME', 'REPLACE', 'REMOVE', 'ADD', 'COPY', 'MOVE', 'FILTER', 'SPLIT', 'MERGE', 'CONCATENATE', 'LOOKUP', 'MAP')
    target_attr_value_id VARCHAR(50) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (rule_id)
);

CREATE TABLE IF NOT EXISTS data_language.feed_attribute_value_mappings (
    rule_id INTEGER NOT NULL,
    step_number INTEGER NOT NULL,
    comparison_operator VARCHAR(10) NOT NULL, -- ENUM('=', '!=', '>', '<', '>=', '<=', 'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL')    
    feed_attribute_id INTEGER NOT NULL,
    feed_attribute_value VARCHAR(50) NOT NULL,
    PRIMARY KEY (rule_id, step_number)
);


