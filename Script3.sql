INSTALL H3 FROM community;
LOAD H3;

FORCE INSTALL SPATIAL FROM 'http://nightly-extensions.duckdb.org';
LOAD SPATIAL;


-- ============================================ 2023 - World (Fishing China-Taiwan)

CREATE OR REPLACE TABLE world AS
	SELECT H3Index,	Flag, ShipType, nb
	FROM read_parquet('/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/density.2023.parquet');

CREATE OR REPLACE TABLE world1 AS
	SELECT 
		H3Index, 
		h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom,
		SUM(nb), nb, Flag, 
		CASE 
            WHEN Flag = 'CN' THEN 'red'
            WHEN Flag = 'TW' THEN 'yellow'
        END AS flag_color
	FROM world
	WHERE (flag='CN' OR flag='TW') AND ShipType='fishing'
	GROUP BY H3Index, geom, nb,	Flag;

ALTER TABLE world ALTER COLUMN H3Index TYPE STRING;

CREATE OR REPLACE TABLE world3 AS
	SELECT 
		h3_h3_to_string(H3Index) AS h3,
		h3_cell_to_lng(h3) AS lng,
		H3Index, nb
	FROM world1
	WHERE lng > -177 AND lng < 177;

CREATE OR REPLACE TABLE world2 AS
	SELECT geom, nb, Flag, flag_color
	FROM world1
	WHERE nb = (
		FROM world3 AS w3
		SELECT MAX(nb)
		WHERE w3.H3Index = world1.H3Index
	);

COPY world2
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/output/density.2023_fishing_CN_TW.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');
