SELECT
map_id
,map_name
,map_description
,from_asset_id
,to_asset_id
,target_attr_value_id
FROM data_language.mapper 
WHERE map_name = '{map_name}'