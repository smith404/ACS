SELECT
map_id
,map_name
,map_description
,from_asset_id
,to_asset_id
,target_attr_value_id
FROM
data_language.mappings
WHERE
map_id = '{map_id}'