SELECT
amr.rule_id
,amr.rule_name
,amr.rule_type
,mrp.step_number  as rule_part_priority
,mrp.comparison_operator
,tda.attribute_name as target_attribute_name
,tda.attribute_type as target_attribute_type
,sda.attribute_name as rule_part__attribute_name
,sda.attribute_type as rule_part__attribute_type
,mrp.feed_attribute_value
FROM
data_language.attribute_mapping_rules amr
,data_language.attribute_mapping_rule_parts mrp
,data_language.data_attributes sda
,data_language.data_attributes tda
WHERE
amr.rule_id = {rule_id}
AND
mrp.rule_id = amr.rule_id
AND
mrp.feed_attribute_id = sda.data_attribute_id
AND
amr.target_attribute_id = tda.data_attribute_id
ORDER BY
mrp.step_number;