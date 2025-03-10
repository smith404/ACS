SELECT
amr.rule_id
,amr.rule_name
,amr.rule_description
,amr.rule_type
,amr.target_attribute_id
,amr.target_attribute_value
,amr.valid_from
,amr.valid_to
FROM
data_language.mapping_rule_assignments mra
,data_language.mapping_rules amr
WHERE
mra.rule_id = amr.rule_id
AND
mra.map_id = {map_id} 