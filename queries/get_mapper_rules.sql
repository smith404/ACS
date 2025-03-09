SELECT
map.map_name
,from_das.data_asset_id as from_asset_id
,from_das.asset_name as from_asset_name
,to_das.data_asset_id as to_asset_id
,to_das.asset_name as to_asset_name
,mra.step_number as rule_priority
,amr.rule_id
,amr.rule_name
,amr.rule_type
,da.data_attribute_id as target_attribute_id
,da.attribute_name as target_attribute_name
,da.attribute_type as target_attribute_type
FROM
data_language.mapper map
,data_language.data_assets from_das
,data_language.data_assets to_das 
,data_language.mapper_rule_assignments mra
,data_language.attribute_mapping_rules amr
,data_language.data_attributes da
WHERE
map.map_name = '{map_name}'
AND
map.from_asset_id = from_das.data_asset_id
AND
map.to_asset_id = to_das.data_asset_id
AND
map.map_id = mra.map_id
and
mra.rule_id = amr.rule_id
AND
amr.target_attr_value_id = da.data_attribute_id
ORDER BY mra.step_number;