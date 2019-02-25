# Reference

For the most up to date list of User Defined Functions using Tiles, look at API documentation for @scaladoc[`RasterFunctions`][RasterFunctions]. 

The full Scala API documentation can be found [here][scaladoc].

RasterFrames also provides SQL and Python bindings to many UDFs using the `Tile` column type. In Spark SQL, the functions are already registered in the SQL engine; they are usually prefixed with `rf_`. In Python, they are available in the `pyrasterframes.rasterfunctions` module. 

The convention in this document will be to define the function signature as below, with its return type, the function name, and named arguments with their types.

```
ReturnDataType function_name(InputDataType argument1, InputDataType argument2)
```

## List of Available SQL and Python Functions

@@toc { depth=3 }

### Vector Operations

Various LocationTech GeoMesa UDFs to deal with `geomtery` type columns are also provided in the SQL engine and within the `pyrasterframes.rasterfunctions` Python module. These are documented in the [LocationTech GeoMesa Spark SQL documentation](https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#). These functions are all prefixed with `st_`.

RasterFrames provides two additional functions for vector geometry.

#### reproject_geometry

_Python_:

    Geometry reproject_geometry(Geometry geom, String origin_crs, String destination_crs)

_SQL_: `rf_reproject_geometry`

Reproject the vector `geom` from `origin_crs` to `destination_crs`. Both `_crs` arguments are either [proj4](https://proj4.org/usage/quickstart.html) strings, [EPSG codes](https://www.epsg-registry.org/) codes or [OGC WKT](https://www.opengeospatial.org/standards/wkt-crs) for coordinate reference systems. 


#### envelope

_Python_:

    Struct[Double minX, Double maxX, Double minY, Double maxY] envelope(Geometry geom)

Python only. Extracts the bounding box (envelope) of the geometry.

See also GeoMesa [st_envelope](https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#st-envelope) which returns a Geometry type.


### Tile Metadata

Functions to access the particulars of a cell: its shape and the data type of its cells. See below section on masking and nodata for additional discussion of cell types.

#### cellTypes


_Python_:

    Array[String] cellTypes()
    
    
_SQL_: `rf_cell_types`

Print an array of possible cell type names, as below. These names are used in other functions. See @ref:[discussion on nodata](reference.md#masking-and-nodata) for additional details.
 
|cellTypes |
|----------|
|bool      |
|int8raw   |
|int8      |
|uint8raw  |
|uint8     |
|int16raw  |
|int16     |
|uint16raw |
|uint16    |
|int32raw  |
|int32     |
|float32raw|
|float32   |
|float64raw|
|float64   |


#### tile_dimensions

_Python_:

    Struct[Int, Int] tile_dimensions(Tile tile)
   
_SQL_: `rf_tile_dimensions`

Get number of columns and rows in the `tile`, as a Struct of `cols` and `rows`.

#### cell_type

_Python_:

    Struct[String] cell_type(Tile tile)
    
_SQL_: `rf_cell_type`

Get the cell type of the `tile`. Available cell types can be retrieved with the @ref:[cellTypes](reference.md#celltypes) function.

#### convert_cell_type

_Python_:

    Tile convert_cell_type(Tile tileCol, String cellType)
    
_SQL_: `rf_convert_cell_type`

Convert `tileCol` to a different cell type.

### Tile Creation

Functions to create a new Tile column, either from scratch or from existing data not yet in a `tile`.

#### tile_zeros

_Python_:

```
Tile tile_zeros(Int tile_columns, Int tile_rows, String cell_type_name)
```

_SQL_: `rf_tile_zeros`

Create a `tile` of shape `tile_columns` by `tile_rows` full of zeros, with the specified cell type. See function @ref:[`cellTypes`](reference.md#celltypes) for valid values. All arguments are literal values and not column expressions.

#### tile_ones

_Python_:

```
Tile tile_ones(Int tile_columns, Int tile_rows, String cell_type_name)
```

_SQL_: `rf_tile_ones`

Create a `tile` of shape `tile_columns` by `tile_rows` full of ones, with the specified cell type. See function @ref:[`cellTypes`](reference.md#celltypes) for valid values. All arguments are literal values and not column expressions.

#### make_constant_tile

_Python_: 

    Tile make_constant_tile(Numeric constant, Int tile_columns, Int tile_rows,  String cell_type_name)
    
_SQL_: `rf_make_constant_tile`

Create a `tile` of shape `tile_columns` by `tile_rows` full of `constant`, with the specified cell type. See function @ref:[`cellTypes`](reference.md#celltypes) for valid values. All arguments are literal values and not column expressions.


#### rasterize

_Python_:

    Tile rasterize(Geometry geom, Geometry tile_bounds, Int value, Int tile_columns, Int tile_rows)
    
_SQL_: `rf_rasterize`

Convert a vector Geometry `geom` into a Tile representation. The `value` will be "burned-in" to the returned `tile` where the `geom` intersects the `tile_bounds`. Returned `tile` will have shape `tile_columns` by `tile_rows`. Values outside the `geom` will be assigned a nodata value. Returned `tile` has cell type `int32`, note that `value` is of type Int.

Parameters `tile_columns` and `tile_rows` are literals, not column expressions. The others are column expressions.


Example use. In the code snip below, you can visualize the `tri` and `b` geometries with tools like [Wicket](https://arthur-e.github.io/Wicket/sandbox-gmaps3.html). The result is a right triangle burned into the `tile`, with nodata values shown as ∘.


```python
spark.sql("""
SELECT rf_render_ascii(
        rf_rasterize(tri, b, 8, 10, 10))

FROM 
  ( SELECT st_geomFromWKT('POLYGON((1.5 0.5, 1.5 1.5, 0.5 0.5, 1.5 0.5))') AS tri,
           st_geomFromWKT('POLYGON((0.0 0.0, 2.0 0.0, 2.0 2.0, 0.0 2.0, 0.0 0.0))') AS b
   ) r
""").show(1, False)

-----------
|∘∘∘∘∘∘∘∘∘∘
∘∘∘∘∘∘∘∘∘∘
∘∘∘∘∘∘∘∘∘∘
∘∘∘∘∘∘∘ ∘∘
∘∘∘∘∘∘  ∘∘
∘∘∘∘∘   ∘∘
∘∘∘∘    ∘∘
∘∘∘     ∘∘
∘∘∘∘∘∘∘∘∘∘
∘∘∘∘∘∘∘∘∘∘|
-----------
```


#### array_to_tile

_Python_:

    Tile array_to_tile(Array arrayCol, Int numCols, Int numRows)
    
Python only. Create a `tile` from a Spark SQL [Array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType), filling values in row-major order.

#### assemble_tile

_Python_:

    Tile assemble_tile(Int colIndex, Int rowIndex, Numeric cellData, Int numCols, Int numRows, String cellType)
    
Python only. Create a Tile from  a column of cell data with location indices. This function is the inverse of @ref:[`explode_tiles`](reference.md#explode-tiles). Intended use is with a `groupby`, producing one row with a new `tile` per group.  The `numCols`, `numRows` and `cellType` arguments are literal values, others are column expressions. Valid values for `cellType` can be found with function @ref:[`cellTypes`](reference.md#celltypes).

### Masking and Nodata

In raster operations, the preservation and correct processing of missing operations is very important. The idea of missing data is often expressed as a null or NaN. In raster data, missing observations are often termed NODATA; we will style them as nodata in this document.  RasterFrames provides a variety of functions to manage and inspect nodata within `tile`s. 

See also statistical summaries to get the count of data and nodata values per `tile` and aggregate in a `tile` column: @ref:[`data_cells`](reference.md#data-cells), @ref:[`no_data_cells`](reference.md#no-data-cells), @ref:[`agg_data_cells`](reference.md#agg-data-cells), @ref:[`agg_no_data_cells`](reference.md#agg-no-data-cells).

It is important to note that not all cell types support the nodata representation: these are `bool` and when the cell type string ends in `raw`.

For integral valued cell types, the nodata is marked by a special sentinel value. This can be a default, typically zero or the minimum value for the underlying data type. The nodata value can also be a user-defined value. For example if the value 4 is to be interpreted as nodata, the cell type will read 'int32ud4'. More discussion in the [GeoTrellis documentation](https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html?#working-with-cell-values).

For float cell types, the nodata can either be NaN or a user-defined vale; for example `'float32ud-999.9'` would mean the value -999.9 is interpreted as a nodata.

#### mask

_Python_:

    Tile mask(Tile tile, Tile mask)
    
_SQL_: `rf_mask`

Where the `mask` contains nodata, replace values in the `tile` with nodata. See [GeoTrellis]

Returned `tile` cell type will be coerced to one supporting nodata if it does not already.
 

#### inverse_mask

_Python_:

    Tile inverse_mask(Tile tile, Tile mask)
    
_SQL_: `rf_inverse_mask`

Where the `mask` _does not_ contain nodata, replace values in `tile` with nodata. 

#### mask_by_value

_Python_:

    Tile mask_by_value(Tile data_tile, Tile mask_tile, Int mask_value)
    
_SQL_: `rf_mask_by_value`

Generate a `tile` with the values from `data_tile`, with nodata in cells where the `mask_tile` is equal to `mask_value`. 


#### rf_is_no_data_tile

_SQL_:

    Boolean rf_is_no_data_tile(tile)

SQL only. Returns true if `tile` contains only nodata. By definition returns false if cell type does not support nodata.

#### with_no_data

_Python_:

    Tile with_no_data(Tile tile, Double no_data_value)
    
Python only. Return a `tile` column marking as nodata all cells equal to `no_data_value`.

The `no_data_value` argument is a literal Double, not a Column expression.

If input `tile` had a nodata value already, the behaviour depends on if its cell type is floating point or not. For floating point cell type `tile`, nodata values on the input `tile` remain nodata values on the output. For integral cell type `tile`s, the previous nodata values become literal values. 

### Map Algebra

[Map algebra](https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html?#map-algebra) raster operations are element-wise operations between a `tile` and a scalar, between two `tile`s, or among many `tile`s. 

Many of these functions have similar variations:

 - `local_op`: applies `op` to two `tile` columns
 - `local_op_scalar`: applies `op` to a `tile` and a literal scalar, coercing the `tile` to a floating point type
 - `local_op_scalar_int`: applies `op` to a `tile` and a literal scalar, without coercing the `tile` to a floating point type
 
We will provide all these variations for `local_add` and then suppress the rest in this document.


#### local_add

_Python_: 
    
    Tile local_add(Tile tile1, Tile tile2)
    
_SQL_: `rf_local_add`

Returns a `tile` column containing the element-wise sum of `tile1` and `tile2`.

#### local_add_scalar

_Python_:

    Tile local_add_scalar(Tile tile, Double scalar)
    
_SQL_: `rf_local_add_scalar`

Returns a `tile` column containing the element-wise sum of `tile` and `scalar`. If `tile` is integral type, it will be coerced to floating before addition; returns float valued `tile`.


#### local_add_scalar_int

_Python_:

    Tile local_add_scalar_int(Tile tile, Int scalar)
    
_SQL_: `rf_local_add_scalar_int`
 
Returns a `tile` column containing the element-wise sum of `tile` and `scalar`. If `tile` is integral type, returns integral type `tile`.

#### local_subtract

_Python_: 
    
    Tile local_subtract(Tile tile1, Tile tile2)
    
_SQL_: `rf_local_subtract`

Returns a `tile` column containing the element-wise difference of `tile1` and `tile2`.


#### local_multiply

_Python_: 
    
    Tile local_multiply(Tile tile1, Tile tile2)
    
_SQL_: `rf_local_multiply`

Returns a `tile` column containing the element-wise product of `tile1` and `tile2`. This is **not** the matrix multiplication of `tile1` and `tile2`.


#### local_divide

_Python_: 
    
    Tile local_divide(Tile tile1, Tile tile2)
    
_SQL_: `rf_local_divide`

Returns a `tile` column containing the element-wise quotient of `tile1` and `tile2`. 


#### normalized_difference 

_Python_:

    Tile normalized_difference(Tile tile1, Tile tile2)
    
_SQL_: `rf_normalized_difference`

Compute the normalized difference of the the two `tile`s: `(tile1 - tile2) / (tile1 + tile2)`. Result is always floating point cell type. This function has no scalar variant. 


#### local_less

_Python_: 
    
    Tile local_less(Tile tile1, Tile tile2)
    
_SQL_: `rf_less`

Returns a `tile` column containing the element-wise evaluation of `tile1` is less than `tile2`. 

#### local_less_equal

_Python_: 
    
    Tile local_less_equal(Tile tile1, Tile tile2)
    
_SQL_: `rf_less_equal`

Returns a `tile` column containing the element-wise evaluation of `tile1` is less than or equal to `tile2`. 

#### local_greater

_Python_: 
    
    Tile local_greater(Tile tile1, Tile tile2)
    
_SQL_: `rf_greater`

Returns a `tile` column containing the element-wise evaluation of `tile1` is greater than `tile2`. 

#### local_greater_equal

_Python_: 
    
    Tile local_greater_equal(Tile tile1, Tile tile2)
    
_SQL_: `rf_greater_equal`

Returns a `tile` column containing the element-wise evaluation of `tile1` is greater than or equal to `tile2`. 

#### local_equal

_Python_: 
    
    Tile local_equal(Tile tile1, Tile tile2)
    
_SQL_: `rf_equal`

Returns a `tile` column containing the element-wise equality of `tile1` and `tile2`. 

#### local_unequal

_Python_: 
    
    Tile local_unequal(Tile tile1, Tile tile2)
    
_SQL_: `rf_unequal`

Returns a `tile` column containing the element-wise inequality of `tile1` and `tile2`. 

### Tile Statistics

The following functions compute a statistical summary per row of a `tile` column. The statistics are computed across the cells of a single `tile`, within each DataFrame Row.  Consider the following example.

```python
import pyspark.functions as F
spark.sql("""
 SELECT 1 as id, rf_tile_ones(5, 5, 'float32') as t 
 UNION
 SELECT 2 as id, rf_local_multiply_scalar(rf_tile_ones(5, 5, 'float32'), 3) as t 
 """).select(F.col('id'), tile_sum(F.col('t'))).show()


+---+-----------+
| id|tile_sum(t)|
+---+-----------+
|  2|       75.0|
|  1|       25.0|
+---+-----------+
```


#### tile_sum

_Python_:

    Double tile_sum(Tile tile)
    
_SQL_:  `rf_tile_sum`

Computes the sum of cells in each row of column `tile`, ignoring nodata values.

#### tile_mean

_Python_:

    Double tile_mean(Tile tile)
    
_SQL_:  `rf_tile_mean`

Computes the mean of cells in each row of column `tile`, ignoring nodata values.


#### tile_min

_Python_:

    Double tile_min(Tile tile)
    
_SQL_:  `rf_tile_min`

Computes the min of cells in each row of column `tile`, ignoring nodata values.


#### tile_max

_Python_:

    Double tile_max(Tile tile)
    
_SQL_:  `rf_tile_max`

Computes the max of cells in each row of column `tile`, ignoring nodata values.


#### no_data_cells

_Python_:

    Long no_data_cells(Tile tile)
    
_SQL_: `rf_no_data_cells`

Return the count of nodata cells in the `tile`.

#### data_cells

_Python_:

    Long data_cells(Tile tile)
    
_SQL_: `rf_data_cells`

Return the count of data cells in the `tile`.

#### tile_stats

_Python_:

    Struct[Long, Long, Double, Double, Double, Double] tile_stats(Tile tile)
    
_SQL_: `tile_stats`

Computes the following statistics of cells in each row of column `tile`: data cell count, nodata cell count, minimum, maximum, mean, and variance. The minimum, maximum, mean, and variance are computed ignoring nodata values. 


#### tile_histogram

_Python_:

    Struct[Struct[Long, Long, Double, Double, Double, Double], Array[Struct[Double, Long]]] tile_histogram(Tile tile)
    
_SQL_:  `rf_tile_histogram`

Computes a statistical summary of cell values within each row of `tile`. Resulting column has the below schema. Note that several of the other `tile` statistics functions are convenience methods to extract parts of this result. Related is the @ref:[`agg_histogram`](reference.md#agg-histogram) which computes the statistics across all rows in a group.

```
 |-- tile_histogram: struct (nullable = true)
 |    |-- stats: struct (nullable = true)
 |    |    |-- dataCells: long (nullable = false)
 |    |    |-- noDataCells: long (nullable = false)
 |    |    |-- min: double (nullable = false)
 |    |    |-- max: double (nullable = false)
 |    |    |-- mean: double (nullable = false)
 |    |    |-- variance: double (nullable = false)
 |    |-- bins: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- value: double (nullable = false)
 |    |    |    |-- count: long (nullable = false)
```

### Aggregate Tile Statistics

These functions compute statistical summaries over all of the cell values *and* across all the rows in the DataFrame or group. Example use below computes a single double-valued mean per month, across all data cells in the `red_band` `tile` type column. This would return at most twelve rows.


```python
from pyspark.functions import month
from pyrasterframes.functions import agg_mean
rf.groupby(month(rf.datetime)).agg(agg_mean(rf.red_band).alias('red_mean_monthly'))
```

Continuing our example from the @ref:[Tile Statistics](reference.md#tile-statistics) section, consider the following. Note that only a single row is returned. It is averaging 25 values of 1.0 and 25 values of 3.0, across the fifty cells in two rows.

```python 
spark.sql("""
SELECT 1 as id, rf_tile_ones(5, 5, 'float32') as t 
UNION
SELECT 2 as id, rf_local_multiply_scalar(rf_tile_ones(5, 5, 'float32'), 3) as t 
""").agg(agg_mean(F.col('t'))).show(10, False)

+-----------+
|agg_mean(t)|
+-----------+
|2.0        |
+-----------+
```

#### agg_mean

_Python_:

    Double agg_mean(Tile tile)
    
_SQL_: @ref:[`rf_agg_stats`](reference.md#agg-stats)`(tile).mean`

Aggregates over the `tile` and return the mean of cell values, ignoring nodata. Equivalent to @ref:[`agg_stats`](reference.md#agg-stats)`.mean`.


#### agg_data_cells

_Python_:

    Long agg_data_cells(Tile tile)
    
_SQL_: @ref:[`rf_agg_stats`](reference.md#agg-stats)`(tile).dataCells`

Aggregates over the `tile` and return the count of data cells. Equivalent to @ref:[`agg_stats`](reference.md#agg-stats)`.dataCells`. C.F. @ref:[`data_cells`](reference.md#data-cells); equivalent code:

```python
rf.select(agg_data_cells(rf.tile).alias('agg_data_cell')).show()
# Equivalent to
rf.agg(F.sum(data_cells(rf.tile)).alias('agg_data_cell')).show()
```

#### agg_no_data_cells

_Python_:

    Long agg_no_data_cells(Tile tile)
    
_SQL_: @ref:[`rf_agg_stats`](reference.md#agg-stats)`(tile).noDataCells`

Aggregates over the `tile` and return the count of nodata cells. Equivalent to @ref:[`agg_stats`](reference.md#agg-stats)`.noDataCells`. C.F. @ref:[`no_data_cells`](reference.md#no-data-cells) a row-wise count of no data cells.

#### agg_stats

_Python_: 

    Struct[Long, Long, Double, Double, Double, Double] agg_stats(Tile tile)
    
_SQL_:  `rf_agg_stats`

Aggregates over the `tile` and returns statistical summaries of cell values: number of data cells, number of nodata cells, minimum, maximum, mean, and variance. The minimum, maximum, mean, and variance ignore the presence of nodata. Equivalent to @ref:[`agg_histogram`](reference.md#agg-histogram)`(tile).stats`.

#### agg_histogram

_Python_:

    Struct[Struct[Long, Long, Double, Double, Double, Double], Array[Struct[Double, Long]]] agg_histogram(Tile tile)
    
_SQL_: `rf_agg_histogram`

Aggregates over the `tile` return statistical summaries of the cell values, including a histogram, in the below schema. The `bins` array is of tuples of histogram values and counts. Typically values are plotted on the x-axis and counts on the y-axis. 

Note that several of the other cell value statistics functions are convenience methods to extract parts of this result. Related is the @ref:[`tile_histogram`](reference.md#tile-histogram) function which operates on a single row at a time.

```
 |-- agg_histogram: struct (nullable = true)
 |    |-- stats: struct (nullable = true)
 |    |    |-- dataCells: long (nullable = false)
 |    |    |-- noDataCells: long (nullable = false)
 |    |    |-- min: double (nullable = false)
 |    |    |-- max: double (nullable = false)
 |    |    |-- mean: double (nullable = false)
 |    |    |-- variance: double (nullable = false)
 |    |-- bins: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- value: double (nullable = false)
 |    |    |    |-- count: long (nullable = false)
```

### Tile Local Aggregate Statistics

Local statistics compute the element-wise statistics across a DataFrame or group of `tile`s, resulting in a `tile` that has the same dimension. 

Consider again our example for Tile Statistics and Aggregate Tile Statistics, this time apply @ref:[`local_agg_mean`](reference.md#local-agg-mean). We see that it is computing the element-wise mean across the two rows. In this case it is computing the mean of one value of 1.0 and one value of 3.0 to arrive at the element-wise mean, but doing so twenty-five times, one for each position in the `tile`.


```python
import pyspark.functions as F
lam = spark.sql("""
SELECT 1 as id, rf_tile_ones(5, 5, 'float32') as t 
UNION
SELECT 2 as id, rf_local_multiply_scalar(rf_tile_ones(5, 5, 'float32'), 3) as t 
""").agg(local_agg_mean(F.col('t')).alias('l')) \

## local_agg_mean returns a tile
lam.select(tile_dimensions(lam.l)).show()
## 
+------------------+
|tile_dimensions(l)|
+------------------+
|            [5, 5]|
+------------------+ 
##

lam.select(explode_tiles(lam.l)).show(10, False)
##
+------------+---------+---+
|column_index|row_index|l  |
+------------+---------+---+
|0           |0        |2.0|
|1           |0        |2.0|
|2           |0        |2.0|
|3           |0        |2.0|
|4           |0        |2.0|
|0           |1        |2.0|
|1           |1        |2.0|
|2           |1        |2.0|
|3           |1        |2.0|
|4           |1        |2.0|
+------------+---------+---+
only showing top 10 rows
```


#### local_agg_max 

_Python_:

    Tile local_agg_max(Tile tile)
    
_SQL_: `rf_local_agg_max`

Compute the cell-local maximum operation over Tiles in a column. 

#### local_agg_min 

_Python_:

    Tile local_agg_min(Tile tile)
    
_SQL_: `rf_local_agg_min`

Compute the cell-local minimum operation over Tiles in a column. 

#### local_agg_mean 

_Python_:

    Tile local_agg_mean(Tile tile)
    
_SQL_: `rf_local_agg_mean`

Compute the cell-local mean operation over Tiles in a column. 

#### local_agg_data_cells 

_Python_:

    Tile local_agg_data_cells(Tile tile)
    
_SQL_: `rf_local_agg_count`

Compute the cell-local count of data cells over Tiles in a column. Returned `tile` has a cell type of `int32`.

#### local_agg_no_data_cells

_Python_:

    Tile local_agg_no_data_cells(Tile tile)
    
 Compute the cell-local count of nodata cells over Tiles in a column. Returned `tile` has a cell type of `int32`.
 Python only.

#### local_agg_stats 

_Python_:

    Struct[Tile, Tile, Tile, Tile, Tile] local_agg_stats(Tile tile)
    
_SQL_: `rf_local_agg_stats`

Compute cell-local aggregate count, minimum, maximum, mean, and variance for a column of Tiles. Returns a struct of five `tile`s.


### Converting Tiles to Other Types

#### explode_tiles

_Python_:

    Int, Int, Numeric* explode_tiles(Tile* tile)
    
_SQL_: `rf_explode_tiles`

Create a row for each cell in `tile` columns. Many `tile` columns can be passed in, and the returned DataFrame will have one numeric column per input.  There will also be columns for `column_index` and `row_index`. Inverse of @ref:[`assemble_tile`](reference.md#assemble-tile). When using this function, be sure to have a unique identifier for rows in order to successfully invert the operation.

#### explode_tiles_sample

_Python_:

    Int, Int, Numeric* explode_tiles_sample(Double sample_frac, Long seed, Tile* tile)
    
Python only. As with @ref:[`explode_tiles`](reference.md#explode-tiles), but taking a randomly sampled subset of cells. Equivalent to the below, but this implementation is optimized for speed. Parameter `sample_frac` should be between 0.0 and 1.0. 

```python
df.select(df.id, explode_tiles(df.tile1, df.tile2, df.tile3)) \
    .sample(False, 0.05, 8675309)
# Equivalent result, faster
df.select(df.id, explode_tiles_sample(0.05, 8675309, df.tile1, df.tile2, df.tile3)) \
```

#### tile_to_int_array

_Python_:

    Array tile_to_int_array(Tile tile)
    
_SQL_: `rf_tile_to_int_array`


Convert Tile column to Spark SQL [Array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType), in row-major order. Float cell types will be coerced to integral type by flooring.


#### tile_to_double_array

_Python_:

    Array tile_to_double_arry(Tile tile)
    
_SQL_: `rf_tile_to_double_array`

Convert tile column to Spark [Array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType), in row-major order. Integral cell types will be coerced to floats.


#### render_ascii

_Python_:

    String render_ascii(Tile tile)

_SQL_:   `rf_render_ascii`

Pretty print the tile values as plain text.



[RasterFunctions]: astraea.spark.rasterframes.RasterFunctions
[scaladoc]: latest/api/index.html


