SELECT
rule_id
,rule_name
,rule_description
,rule_type
,target_attribute_id
,target_attribute_value
,valid_from
,valid_to
FROM
data_language.mapping_rules
WHERE
rule_id = '{rule_id}'