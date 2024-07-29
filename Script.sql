LOAD H3;
LOAD SPATIAL ;

--  =========================================== 012.5

CREATE OR REPLACE TABLE 'density12.5' AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/density.012.5.parquet';

ALTER TABLE 'density12.5' ALTER COLUMN H3Index TYPE STRING;

-- most_represented_shipType_in_a_cell
CREATE OR REPLACE TABLE groupByType AS
	SELECT H3Index, geom, ShipType, SUM(nb), nb
	FROM 'density12.5'
	GROUP BY H3Index, geom, ShipType, nb;

CREATE OR REPLACE TABLE most_represented_shipType_in_a_cell AS
	SELECT H3Index, geom, ShipType, nb
	FROM groupByType
	WHERE
	nb = (
			FROM 'density12.5' AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByType.H3Index
			AND (ShipType = 'tanker'
			OR ShipType = 'cargo'
			OR ShipType = 'passenger'
			OR ShipType = 'fishing')
);

COPY most_represented_shipType_in_a_cell 
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/density.012.5_most_represented_shipType.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- most_represented_flag_in_a_cell
CREATE OR REPLACE TABLE groupByFlag AS
	SELECT H3Index, geom, Flag, SUM(nb), nb
	FROM 'density12.5'
	GROUP BY H3Index, geom, Flag, nb;

CREATE OR REPLACE TABLE most_represented_flag_in_a_cell AS
	SELECT H3Index, geom, Flag, nb
	FROM groupByFlag
	WHERE nb = (
			FROM 'density12.5' AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByFlag.H3Index
	);

COPY most_represented_flag_in_a_cell
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/density.012.5_most_represented_flag.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- ============================================ 012

CREATE OR REPLACE TABLE density12 AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/density.012.parquet';

ALTER TABLE density12 ALTER COLUMN H3Index TYPE STRING;

-- most_represented_shipType_in_a_cellDensity12

CREATE OR REPLACE TABLE groupByTypeDensity12 AS
	SELECT H3Index, geom, ShipType, SUM(nb), nb
	FROM density12
	GROUP BY H3Index, geom, ShipType, nb;

CREATE OR REPLACE TABLE most_represented_shipType_in_a_cellDensity12 AS
	SELECT H3Index, geom, ShipType, nb
	FROM groupByTypeDensity12
	WHERE
	nb = (
			FROM density12 AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByTypeDensity12.H3Index
			AND (ShipType = 'tanker'
			OR ShipType = 'cargo'
			OR ShipType = 'passenger'
			OR ShipType = 'fishing')
);

COPY most_represented_shipType_in_a_cellDensity12 
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/density.012_most_represented_shipType.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- most_represented_flag_in_a_cellDensity12

CREATE OR REPLACE TABLE groupByFlagDensity12 AS
	SELECT H3Index, geom, Flag, SUM(nb), nb
	FROM density12
	GROUP BY H3Index, geom, Flag, nb;

CREATE OR REPLACE TABLE most_represented_flag_in_a_cellDensity12 AS
	SELECT H3Index, geom, Flag, nb
	FROM groupByFlagDensity12
	WHERE nb = (
			FROM density12 AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByFlagDensity12.H3Index
	);

COPY most_represented_flag_in_a_cellDensity12 
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/density.012_most_represented_flag.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- Densité par cellule

CREATE OR REPLACE TABLE table1 AS
	SELECT H3Index, geom, SUM(nb), nb
	FROM density12
	GROUP BY H3Index, geom, nb;

CREATE OR REPLACE TABLE table2 AS
	SELECT H3Index, geom, nb
	FROM table1
	WHERE nb = (
			FROM density12 AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = table1.H3Index
	);

COPY table2
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/density.012.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

--classify field=nb colors="Oranges"

-- Densité par cellule + ShipType mis en exergue (fishing, tanker, cargo, other)

CREATE OR REPLACE TABLE table3 AS
	SELECT H3Index, geom, SUM(nb), nb, ShipType,
		CASE 
            WHEN ShipType = 'fishing' THEN '#f78851'
            WHEN ShipType = 'cargo' THEN '#78f48b'
            WHEN ShipType = 'tanker' THEN '#ff272d'
            ELSE '#e1cf15'
        END AS color
	FROM density12
	GROUP BY H3Index, geom, nb, ShipType;

CREATE OR REPLACE TABLE table4 AS
	SELECT H3Index, geom, nb, ShipType, color
	FROM table3
	WHERE nb = (
			FROM density12 AS h
			SELECT MAX(nb)
			WHERE h.H3Index = table3.H3Index
	);

COPY table4
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/output/density.012_shipType.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');


--classify field=nb colors=#feeddeaa,#fdbe85aa,#fd8d3caa,#d94701aa

--style stroke=color

--dissolve stroke,ShipType

--style fill=stroke

-- ============================================ 015

CREATE OR REPLACE TABLE density015 AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/density.015.parquet';

ALTER TABLE density015 ALTER COLUMN H3Index TYPE STRING;

CREATE OR REPLACE TABLE table5 AS
	SELECT H3Index, geom, SUM(nb), nb, ShipType,
		CASE 
            WHEN ShipType = 'fishing' THEN '#f78851'
            WHEN ShipType = 'cargo' THEN '#78f48b'
            WHEN ShipType = 'tanker' THEN '#ff272d'
            ELSE '#e1cf15'
        END AS color
	FROM density015
	GROUP BY H3Index, geom, nb, ShipType;

CREATE OR REPLACE TABLE table6 AS
	SELECT H3Index, geom, nb, ShipType, color
	FROM table5
	WHERE nb = (
			FROM density015 AS h
			SELECT MAX(nb)
			WHERE h.H3Index = table5.H3Index
	);

COPY table6
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/output/density.015_shipType.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

--merge-layers target='table4,table6'

--merge-layers target='density.012_shipType,density.015_shipType'

--classify field=nb colors=#feeddeaa,#fdbe85aa,#fd8d3caa,#d94701aa

--style stroke=color

--dissolve stroke,ShipType

--style fill=stroke


-- ============================================ 028

CREATE OR REPLACE TABLE density028 AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/density.028.parquet';

ALTER TABLE density028 ALTER COLUMN H3Index TYPE STRING;

CREATE OR REPLACE TABLE table7 AS
	SELECT H3Index, geom, SUM(nb), nb, ShipType,
		CASE 
            WHEN ShipType = 'fishing' THEN '#f78851'
            WHEN ShipType = 'cargo' THEN '#78f48b'
            WHEN ShipType = 'tanker' THEN '#ff272d'
            ELSE '#e1cf15'
        END AS color
	FROM density028
	GROUP BY H3Index, geom, nb, ShipType;

CREATE OR REPLACE TABLE table8 AS
	SELECT H3Index, geom, nb, ShipType, color
	FROM table7
	WHERE nb = (
			FROM density028 AS h
			SELECT MAX(nb)
			WHERE h.H3Index = table7.H3Index
	);

COPY table8
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/output/density.028_shipType.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- ============================================ 031

CREATE OR REPLACE TABLE density031 AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/PARQUET/density.031.parquet';

ALTER TABLE density031 ALTER COLUMN H3Index TYPE STRING;

CREATE OR REPLACE TABLE table9 AS
	SELECT H3Index, geom, SUM(nb), nb, ShipType,
		CASE 
            WHEN ShipType = 'fishing' THEN '#f78851'
            WHEN ShipType = 'cargo' THEN '#78f48b'
            WHEN ShipType = 'tanker' THEN '#ff272d'
            ELSE '#e1cf15'
        END AS color
	FROM density031
	GROUP BY H3Index, geom, nb, ShipType;

CREATE OR REPLACE TABLE table10 AS
	SELECT H3Index, geom, nb, ShipType, color
	FROM table9
	WHERE nb = (
			FROM density031 AS h
			SELECT MAX(nb)
			WHERE h.H3Index = table9.H3Index
	);

COPY table10
TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/JSON/output/density.031_shipType.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

--merge-layers target='density.012_shipType,density.015_shipType,density.028_shipType,density.031_shipType'

--classify field=nb colors=#feeddeaa,#fdbe85aa,#fd8d3caa,#d94701aa

--style stroke=color

--dissolve stroke,ShipType

--style fill=stroke
