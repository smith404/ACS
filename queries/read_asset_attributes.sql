SELECT
data_attribute_id
,attribute_name
,attribute_description
,attribute_type
,attribute_length
,external_source
FROM
data_language.data_attributes da,
data_language.data_asset_attributes daa
WHERE
da.data_attribute_id = daa.data_attribute_id
AND
daa.data_asset_id = {data_asset_id}