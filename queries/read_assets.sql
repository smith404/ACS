SELECT
data_asset_id
,domain
,asset_name
,asset_description
,medallion_layer
,container
,feed_path
FROM data_language.data_assets
WHERE asset_name = '{asset_name}'