# duckdb-mapshaper

List of tools and resources for working with DuckDB and Mapshaper. SQL scripts allows to manipulate ship density data and flag by H3 cells. Then, the data can be exported to a GeoJSON file and displayed on a map using Mapshaper.

## DBeaver
Tool for database management and SQL queries, compatible with DuckDB.

## Parquet file
Parquet is a file format that provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk (compared to CSV or JSON files for example).

It can be read easily by DuckDB.

```sql
SELECT * FROM 'file.parquet';
```

## DuckDB
Fast SQL database management system. 

- To use spatial and H3 functions, you need to install theses extensions :

```sql
INSTALL H3 FROM community;
LOAD H3;

FORCE INSTALL SPATIAL FROM 'http://nightly-extensions.duckdb.org';
LOAD SPATIAL ;
```

- To manage anti-meridian, you can choose an interval for the longitude values :
    
```sql
-- Code snippet to manage anti-meridian

... h3_h3_to_string(H3Index) AS h3 ... -- convert into a H3 string

... h3_cell_to_lng(h3) AS lng ... -- get the longitude value from the H3 string

... lng > -177 AND lng < 177 ... -- filter the longitude values between -177 and 177

```

- To export a table to a GeoJSON file, you can use the following command :

```sql
COPY table_name
TO 'file.json'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');
```

## Mapshaper

https://mapshaper.org/

Tool for editing and converting shapefiles, GeoJSON ... and displaying them on a map.

A command line tool is also available in the interface.

#### Examples of command line usage
```bash
# Set stroke and fill colors for the features
classify field=ShipType colors="random" \
style stroke=grey

classify field=ShipType colors="#83bcb6AA,#4c78a8AA,#f58518AA,#f2cf5bAA,#9ecae9AA,#54a24bAA,#d6a5c9AA" \
style stroke=d.fill

# Merge layers
--merge-layers target='layer1,layer2'

# Dissolve all features into a single feature by a common field
dissolve stroke
```

![Mapshaper](mapshaper.png)
