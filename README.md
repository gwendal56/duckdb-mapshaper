# duckdb-mapshaper

List of tools and resources for working with DuckDB and Mapshaper.

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

To use spatial and H3 functions, you need to install theses extensions.

```sql
INSTALL H3 FROM community;
LOAD H3;

FORCE INSTALL SPATIAL FROM 'http://nightly-extensions.duckdb.org';
LOAD SPATIAL ;
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

# Dissolve all features into a single feature by a common field
dissolve stroke
```
