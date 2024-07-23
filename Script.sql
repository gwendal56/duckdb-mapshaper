--In MapShaper :
--classify field=ShipType colors="random" \
--style stroke=grey

--classify field=ShipType colors="#83bcb6AA,#4c78a8AA,#f58518AA,#f2cf5bAA,#9ecae9AA,#54a24bAA,#d6a5c9AA" \
--style stroke=d.fill

INSTALL H3 FROM community;
LOAD H3;

FORCE INSTALL SPATIAL FROM 'http://nightly-extensions.duckdb.org';
LOAD SPATIAL ;

-- 012.5
CREATE OR REPLACE TABLE hexagons AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/012.5.density.4.flag.type.parquet';

ALTER TABLE hexagons ALTER COLUMN H3Index TYPE STRING;


COPY hexagons TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/012.5.density.4.flag.type.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- =========================================== most_represented_shipType_in_a_cell
CREATE OR REPLACE TABLE groupByType AS
	SELECT H3Index, geom, ShipType, SUM(nb), nb
	FROM hexagons
	GROUP BY H3Index, geom, ShipType, nb;

CREATE OR REPLACE TABLE most_represented_shipType_in_a_cell AS
	SELECT *
	FROM groupByType
	WHERE
	nb = (
			FROM hexagons AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByType.H3Index
			AND (ShipType = 'tanker'
			OR ShipType = 'cargo'
			OR ShipType = 'passenger'
			OR ShipType = 'fishing')
);
COPY (
	SELECT H3Index, geom, ShipType, nb
	FROM most_represented_shipType_in_a_cell 
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/most_represented_shipType_in_a_cell.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');


-- =========================================== most_represented_flag_in_a_cell
CREATE OR REPLACE TABLE groupByFlag AS
	SELECT H3Index, geom, Flag, SUM(nb), nb
	FROM hexagons
	GROUP BY H3Index, geom, Flag, nb;

CREATE OR REPLACE TABLE most_represented_flag_in_a_cell AS
	SELECT *
	FROM groupByFlag
	WHERE nb = (
			FROM hexagons AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByFlag.H3Index
	);

COPY (
	SELECT H3Index, geom, Flag, nb
	FROM most_represented_flag_in_a_cell 
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/most_represented_flag_in_a_cell.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');
--===============================================================================================================================
--===============================================================================================================================
--012
CREATE OR REPLACE TABLE density12 AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/density.012.parquet';

ALTER TABLE density12 ALTER COLUMN H3Index TYPE STRING;


COPY density12 TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/density.012.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- =========================================== most_represented_shipType_in_a_cellDensity12

CREATE OR REPLACE TABLE groupByTypeDensity12 AS
	SELECT H3Index, geom, ShipType, SUM(nb), nb
	FROM density12
	GROUP BY H3Index, geom, ShipType, nb;

CREATE OR REPLACE TABLE most_represented_shipType_in_a_cellDensity12 AS
	SELECT *
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
COPY (
	SELECT H3Index, geom, ShipType, nb
	FROM most_represented_shipType_in_a_cellDensity12 
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/most_represented_shipType_in_a_cellDensity12.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- =========================================== most_represented_flag_in_a_cellDensity12

CREATE OR REPLACE TABLE groupByFlagDensity12 AS
	SELECT H3Index, geom, Flag, SUM(nb), nb
	FROM density12
	GROUP BY H3Index, geom, Flag, nb;

CREATE OR REPLACE TABLE most_represented_flag_in_a_cellDensity12 AS
	SELECT *
	FROM groupByFlagDensity12
	WHERE nb = (
			FROM density12 AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = groupByFlagDensity12.H3Index
	);

COPY (
	SELECT H3Index, geom, Flag, nb
	FROM most_represented_flag_in_a_cellDensity12 
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/most_represented_flag_in_a_cellDensity12.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- =========================================== Densité par cellule

CREATE OR REPLACE TABLE table1 AS
	SELECT H3Index, geom, SUM(nb), nb
	FROM density12
	GROUP BY H3Index, geom, nb;

CREATE OR REPLACE TABLE table2 AS
	SELECT *
	FROM table1
	WHERE nb = (
			FROM density12 AS h
			SELECT MAX(nb) 
			WHERE h.H3Index = table1.H3Index
	);

COPY (
	SELECT H3Index, geom, nb
	FROM table2 
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/table2.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

$ --classify field=nb colors="Oranges" \
--style stroke=d.fill

-- =========================================== Densité par cellule + ShipType mis en exergue (fishing, tanker, cargo, other)

CREATE OR REPLACE TABLE table3 AS
	SELECT H3Index, geom, SUM(nb), nb, ShipType,
		CASE 
            WHEN ShipType = 'fishing' THEN '#dd944a'
            WHEN ShipType = 'cargo' THEN '#f9c666'
            WHEN ShipType = 'tanker' THEN '#ffe577'
            ELSE '#ffd1a2'
        END AS color
	FROM density12
	GROUP BY H3Index, geom, nb, ShipType;

CREATE OR REPLACE TABLE table4 AS
	SELECT *, ShipType
	FROM table3
	WHERE nb = (
			FROM density12 AS h
			SELECT MAX(nb)
			WHERE h.H3Index = table3.H3Index
	);

COPY (
	SELECT H3Index, geom, nb, ShipType, color
	FROM table4
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/table4.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');


--classify field=nb colors=#feeddeaa,#fdbe85aa,#fd8d3caa,#d94701aa

--style stroke=color

--dissolve stroke,ShipType

--style fill=stroke

CREATE OR REPLACE TABLE density015 AS
	SELECT H3Index, h3_cell_to_boundary_wkt(H3Index).st_geomfromtext() AS geom, Flag, ShipType, nb
	FROM '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/density.015.parquet';

ALTER TABLE density015 ALTER COLUMN H3Index TYPE STRING;

--
COPY density015 TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/density.015.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

CREATE OR REPLACE TABLE table5 AS
	SELECT H3Index, geom, SUM(nb), nb, ShipType,
		CASE 
            WHEN ShipType = 'fishing' THEN '#dd944a'
            WHEN ShipType = 'cargo' THEN '#f9c666'
            WHEN ShipType = 'tanker' THEN '#ffe577'
            ELSE '#ffd1a2'
        END AS color
	FROM density015
	GROUP BY H3Index, geom, nb, ShipType;

CREATE OR REPLACE TABLE table6 AS
	SELECT *, ShipType
	FROM table5
	WHERE nb = (
			FROM density015 AS h
			SELECT MAX(nb)
			WHERE h.H3Index = table5.H3Index
	);

COPY (
	SELECT H3Index, geom, nb, ShipType, color
	FROM table6
) TO '/Users/gwendalleguellec/Documents/DuckDB/density_flag_type/table6.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- 

--merge-layers target='table4,table6'

--classify field=nb colors=#feeddeaa,#fdbe85aa,#fd8d3caa,#d94701aa

--style stroke=color

--dissolve stroke,ShipType

--style fill=stroke