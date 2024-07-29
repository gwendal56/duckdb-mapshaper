LOAD H3;
LOAD SPATIAL;

SET file_search_path = '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/';

-- ============================================ 2023 - World (Fishing China-Taiwan)

CREATE OR REPLACE TABLE world_raw AS
	SELECT * 
	FROM 'density.2023.parquet';

CREATE OR REPLACE TABLE cn_tw AS
	SELECT 
		H3Index, 
		h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom,
		SUM(nb), nb, Flag, 
		CASE 
            WHEN Flag = 'CN' THEN 'red'
            WHEN Flag = 'TW' THEN 'yellow'
        END AS flag_color
	FROM world_raw
	WHERE (flag='CN' OR flag='TW') AND ShipType='fishing'
	GROUP BY H3Index, geom, nb,	Flag;

ALTER TABLE world_raw ALTER COLUMN H3Index TYPE STRING;

CREATE OR REPLACE TABLE anti_meridian AS
	SELECT 
		h3_h3_to_string(H3Index) AS h3,
		h3_cell_to_lng(h3) AS lng,
		H3Index, nb
	FROM cn_tw
	WHERE lng > -177 AND lng < 177;

CREATE OR REPLACE TABLE fishing_CN_TW AS
	SELECT geom, nb, Flag, flag_color
	FROM cn_tw
	WHERE nb = (
		FROM anti_meridian AS w3
		SELECT MAX(nb)
		WHERE w3.H3Index = cn_tw.H3Index
	);

COPY fishing_CN_TW
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/output/density.2023_fishing_CN_TW.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');
