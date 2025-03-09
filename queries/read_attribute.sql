SELECT
data_attribute_id
,attribute_name
,attribute_description
,attribute_type
,attribute_length
,external_source
FROM
data_language.data_attributes
WHERE
data_attribute_id = {data_attribute_id}