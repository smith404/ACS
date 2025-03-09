SELECT
rule_id
,step_number
,comparison_operator  
,feed_attribute_id
,feed_attribute_value
FROM
data_language.attribute_mapping_rule_parts
WHERE
rule_id = '{rule_id}'
ORDER BY
step_number;