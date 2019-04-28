# Function Reference

For the most up to date list of User Defined Functions using Tiles, look at API documentation for @scaladoc[`RasterFunctions`][RasterFunctions]. 

The full Scala API documentation can be found [here][scaladoc].

RasterFrames also provides SQL and Python bindings to many UDFs using the `Tile` column type. In Spark SQL, the functions are already registered in the SQL engine; they are usually prefixed with `rf_`. In Python, they are available in the `pyrasterframes.rasterfunctions` module. 

The convention in this document will be to define the function signature as below, with its return type, the function name, and named arguments with their types.

```
ReturnDataType function_name(InputDataType argument1, InputDataType argument2)
```

## List of Available SQL and Python Functions

The convention in this document will be to define the function signature as below, with its return type, the function name, and named arguments with their types.

```
ReturnDataType function_name(InputDataType argument1, InputDataType argument2)
```

@@toc { depth=3 }

### Vector Operations

Various LocationTech GeoMesa UDFs to deal with `geomtery` type columns are also provided in the SQL engine and within the `pyrasterframes.rasterfunctions` Python module. These are documented in the [LocationTech GeoMesa Spark SQL documentation](https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#). These functions are all prefixed with `st_`.

RasterFrames provides two additional functions for vector geometry.

#### reproject_geometry

_Python_:
    Geometry reproject_geometry(Geometry geom, String origin_crs, String destination_crs)

_SQL_: `rf_reproject_geometry`

Reproject the vector `geom` from `origin_crs` to `destination_crs`. Both `_crs` arguments are either [proj4](https://proj4.org/usage/quickstart.html) strings, [EPSG codes](https://www.epsg-registry.org/) codes or [OGC WKT](https://www.opengeospatial.org/standards/wkt-crs) for coordinate reference systems. 


#### rf_extent

_Python_:

    Struct[Double xmin, Double xmax, Double ymin, Double ymax] rf_extent(Geometry geom)

Extracts the bounding box (extent/envelope) of the geometry.

See also GeoMesa [st_envelope](https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#st-envelope) which returns a Geometry type.

### Tile Metadata and Mutation

Functions to access and change the particulars of a `tile`: its shape and the data type of its cells. See below section on @ref:[masking and nodata](reference.md#masking-and-nodata) for additional discussion of cell types.

#### rf_cell_types


_Python_:

    Array[String] rf_cell_types()
    
_SQL_: `rf_cell_types`

Print an array of possible cell type names, as below. These names are used in other functions. See @ref:[discussion on nodata](reference.md#masking-and-nodata) for additional details.
 
|cell_types |
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


#### rf_tile_dimensions

_Python_:

    Struct[Int, Int] rf_tile_dimensions(Tile tile)
   
_SQL_: `rf_tile_dimensions`

Get number of columns and rows in the `tile`, as a Struct of `cols` and `rows`.

#### rf_cell_type

_Python_:

    Struct[String] rf_cell_type(Tile tile)
    
_SQL_: `rf_cell_type`

Get the cell type of the `tile`. Available cell types can be retrieved with the @ref:[rf_cell_types](reference.md#rf-cell-types) function.

#### rf_convert_cell_type

_Python_:

    Tile rf_convert_cell_type(Tile tileCol, String cellType)
    
_SQL_: `rf_convert_cell_type`

Convert `tileCol` to a different cell type.

#### rf_resample

_Python_: 

    Tile rf_resample(Tile tile, Double factor)
    Tile rf_resample(Tile tile, Int factor)
    Tile rf_resample(Tile tile, Tile shape_tile)
    
_SQL_: `rf_resample`

Change the tile dimension. Passing a numeric `factor` will scale the number of columns and rows in the tile: 1.0 is the same number of columns and row; less than one downsamples the tile; and greater than one upsamples the tile. Passing a `shape_tile` as the second argument outputs `tile` having the same number of columns and rows as `shape_tile`. All resampling is by nearest neighbor method. 

### Tile Creation

Functions to create a new Tile column, either from scratch or from existing data not yet in a `tile`.

#### rf_make_zeros_tile

_Python_:

```
Tile rf_make_zeros_tile(Int tile_columns, Int tile_rows, String cell_type_name)
```

_SQL_: `rf_make_zeros_tile`

Create a `tile` of shape `tile_columns` by `tile_rows` full of zeros, with the specified cell type. See function @ref:[`rf_cell_types`](reference.md#rf-cell-types) for valid values. All arguments are literal values and not column expressions.

#### rf_make_ones_tile

_Python_:

```
Tile rf_make_ones_tile(Int tile_columns, Int tile_rows, String cell_type_name)
```

_SQL_: `rf_make_ones_tile`

Create a `tile` of shape `tile_columns` by `tile_rows` full of ones, with the specified cell type. See function @ref:[`rf_cell_types`](reference.md#rf-cell-types) for valid values. All arguments are literal values and not column expressions.

#### rf_make_constant_tile

_Python_: 

    Tile rf_make_constant_tile(Numeric constant, Int tile_columns, Int tile_rows,  String cell_type_name)
    
_SQL_: `rf_make_constant_tile`

Create a `tile` of shape `tile_columns` by `tile_rows` full of `constant`, with the specified cell type. See function @ref:[`rf_cell_types`](reference.md#rf-cell-types) for valid values. All arguments are literal values and not column expressions.


#### rf_rasterize

_Python_:

    Tile rf_rasterize(Geometry geom, Geometry tile_bounds, Int value, Int tile_columns, Int tile_rows)
    
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


#### rf_array_to_tile

_Python_:

    Tile rf_array_to_tile(Array arrayCol, Int numCols, Int numRows)
    
Python only. Create a `tile` from a Spark SQL [Array](http://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType), filling values in row-major order.

#### rf_assemble_tile

_Python_:

    Tile rf_assemble_tile(Int colIndex, Int rowIndex, Numeric cellData, Int numCols, Int numRows, String cellType)
    
Python only. Create a Tile from  a column of cell data with location indices. This function is the inverse of @ref:[`rf_explode_tiles`](reference.md#rf-explode-tiles). Intended use is with a `groupby`, producing one row with a new `tile` per group.  The `numCols`, `numRows` and `cellType` arguments are literal values, others are column expressions. Valid values for `cellType` can be found with function @ref:[`rf_cell_types`](reference.md#rf-cell-types).

### Masking and Nodata

In raster operations, the preservation and correct processing of missing operations is very important. The idea of missing data is often expressed as a null or NaN. In raster data, missing observations are often termed NODATA; we will style them as nodata in this document.  RasterFrames provides a variety of functions to manage and inspect nodata within `tile`s. 

See also statistical summaries to get the count of data and nodata values per `tile` and aggregate in a `tile` column: @ref:[`rf_data_cells`](reference.md#rf-data-cells), @ref:[`rf_no_data_cells`](reference.md#rf-no-data-cells), @ref:[`rf_agg_data_cells`](reference.md#rf-agg-data-cells), @ref:[`rf_agg_no_data_cells`](reference.md#rf-agg-no-data-cells).

It is important to note that not all cell types support the nodata representation: these are `bool` and when the cell type string ends in `raw`.

For integral valued cell types, the nodata is marked by a special sentinel value. This can be a default, typically zero or the minimum value for the underlying data type. The nodata value can also be a user-defined value. For example if the value 4 is to be interpreted as nodata, the cell type will read 'int32ud4'. 

For float cell types, the nodata can either be NaN or a user-defined value; for example `'float32ud-999.9'` would mean the value -999.9 is interpreted as a nodata.

For more reading about cell types and ndodata, see the [GeoTrellis documentation](https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html?#working-with-cell-values).

#### rf_mask

_Python_:

    Tile rf_mask(Tile tile, Tile mask)
    
_SQL_: `rf_mask`

Where the `mask` contains nodata, replace values in the `tile` with nodata.

Returned `tile` cell type will be coerced to one supporting nodata if it does not already.
 

#### rf_inverse_mask

_Python_:

    Tile rf_inverse_mask(Tile tile, Tile mask)
    
_SQL_: `rf_inverse_mask`

Where the `mask` _does not_ contain nodata, replace values in `tile` with nodata. 

#### rf_mask_by_value

_Python_:

    Tile rf_mask_by_value(Tile data_tile, Tile mask_tile, Int mask_value)
    
_SQL_: `rf_mask_by_value`

Generate a `tile` with the values from `data_tile`, with nodata in cells where the `mask_tile` is equal to `mask_value`. 


#### rf_is_no_data_tile

_Python_:

    Boolean rf_is_no_data_tile(Tile)

_SQL_: `rf_is_no_data_tile`
 
Returns true if `tile` contains only nodata. By definition returns false if cell type does not support nodata.

#### rf_with_no_data

_Python_:

    Tile rf_with_no_data(Tile tile, Double no_data_value)
    
Python only. Return a `tile` column marking as nodata all cells equal to `no_data_value`.

The `no_data_value` argument is a literal Double, not a Column expression.

If input `tile` had a nodata value already, the behaviour depends on if its cell type is floating point or not. For floating point cell type `tile`, nodata values on the input `tile` remain nodata values on the output. For integral cell type `tile`s, the previous nodata values become literal values. 

### Map Algebra

[Map algebra](https://gisgeography.com/map-algebra-global-zonal-focal-local/) raster operations are element-wise operations between a `tile` and a scalar, between two `tile`s, or among many `tile`s. 

Some of these functions have similar variations in the Python API:

 - `rf_local_op`: applies `op` to two columns; the right hand side can be a `tile` or a numeric column.
 - `rf_local_op_double`: applies `op` to a `tile` and a literal scalar, coercing the `tile` to a floating point type
 - `rf_local_op_int`: applies `op` to a `tile` and a literal scalar, without coercing the `tile` to a floating point type
 
We will provide all these variations for `rf_local_add` and then suppress the rest in this document.

The SQL API does not require the `rf_local_op_double` or `rf_local_op_int` forms (just `rf_local_op`).

#### rf_local_add

_Python_: 
    
    Tile rf_local_add(Tile tile1, Tile rhs)
    Tile rf_local_add_int(Tile tile1, Int rhs)
    Tile rf_local_add_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_add`

Returns a `tile` column containing the element-wise sum of `tile1` and `rhs`.

#### rf_local_subtract

_Python_: 
    
    Tile rf_local_subtract(Tile tile1, Tile rhs)
    Tile rf_local_subtract_int(Tile tile1, Int rhs)
    Tile rf_local_subtract_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_subtract`

Returns a `tile` column containing the element-wise difference of `tile1` and `rhs`.


#### rf_local_multiply

_Python_: 
    
    Tile rf_local_multiply(Tile tile1, Tile rhs)
    Tile rf_local_multiply_int(Tile tile1, Int rhs)
    Tile rf_local_multiply_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_multiply`

Returns a `tile` column containing the element-wise product of `tile1` and `rhs`. This is **not** the matrix multiplication of `tile1` and `rhs`.


#### rf_local_divide

_Python_: 
    
    Tile rf_local_divide(Tile tile1, Tile rhs)
    Tile rf_local_divide_int(Tile tile1, Int rhs)
    Tile rf_local_divide_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_divide`

Returns a `tile` column containing the element-wise quotient of `tile1` and `rhs`. 


#### rf_normalized_difference 

_Python_:

    Tile rf_normalized_difference(Tile tile1, Tile tile2)
    
_SQL_: `rf_normalized_difference`

Compute the normalized difference of the the two `tile`s: `(tile1 - tile2) / (tile1 + tile2)`. Result is always floating point cell type. This function has no scalar variant. 

#### rf_local_less

_Python_: 
    
    Tile rf_local_less(Tile tile1, Tile rhs)
    Tile rf_local_less_int(Tile tile1, Int rhs)
    Tile rf_local_less_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_less`

Returns a `tile` column containing the element-wise evaluation of `tile1` is less than `rhs`. 

#### rf_local_less_equal

_Python_: 
    
    Tile rf_local_less_equal(Tile tile1, Tile rhs)
    Tile rf_local_less_equal_int(Tile tile1, Int rhs)
    Tile rf_local_less_equal_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_less_equal`

Returns a `tile` column containing the element-wise evaluation of `tile1` is less than or equal to `rhs`. 

#### rf_local_greater

_Python_: 
    
    Tile rf_local_greater(Tile tile1, Tile rhs)
    Tile rf_local_greater_int(Tile tile1, Int rhs)
    Tile rf_local_greater_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_greater`

Returns a `tile` column containing the element-wise evaluation of `tile1` is greater than `rhs`. 

#### rf_local_greater_equal

_Python_: 
    
    Tile rf_local_greater_equal(Tile tile1, Tile rhs)
    Tile rf_local_greater_equal_int(Tile tile1, Int rhs)
    Tile rf_local_greater_equal_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_greater_equal`

Returns a `tile` column containing the element-wise evaluation of `tile1` is greater than or equal to `rhs`. 

#### rf_local_equal

_Python_: 
    
    Tile rf_local_equal(Tile tile1, Tile rhs)
    Tile rf_local_equal_int(Tile tile1, Int rhs)
    Tile rf_local_equal_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_equal`

Returns a `tile` column containing the element-wise equality of `tile1` and `rhs`. 

#### rf_local_unequal

_Python_: 
    
    Tile rf_local_unequal(Tile tile1, Tile rhs)
    Tile rf_local_unequal_int(Tile tile1, Int rhs)
    Tile rf_local_unequal_double(Tile tile1, Double rhs)
    
_SQL_: `rf_local_unequal`

Returns a `tile` column containing the element-wise inequality of `tile1` and `rhs`. 

#### rf_round

_Python_:

    Tile rf_round(Tile tile)

_SQL_: `rf_round`

Round cell values to the nearest integer without changing the cell type.

#### rf_exp

_Python_:

    Tile rf_exp(Tile tile)

_SQL_: `rf_exp`

Performs cell-wise exponential.

#### rf_exp10

_Python_:

    Tile rf_exp10(Tile tile)

_SQL_: `rf_exp10`

Compute 10 to the power of cell values.

#### rf_exp2

_Python_:

    Tile rf_exp2(Tile tile)

_SQL_: `rf_exp2`

Compute 2 to the power of cell values.

#### rf_expm1

_Python_:

    Tile rf_expm1(Tile tile)

_SQL_: `rf_expm1`

Performs cell-wise exponential, then subtract one. Inverse of @ref:[`log1p`](reference.md#log1p).

#### rf_log

_Python_:

    Tile rf_log(Tile tile)

_SQL_: `rf_log`

Performs cell-wise natural logarithm.

#### rf_log10

_Python_:

    Tile rf_log10(Tile tile)

_SQL_: `rf_log10`

Performs cell-wise logarithm with base 10.

#### rf_log2

_Python_: 

    Tile rf_log2(Tile tile)

_SQL_: `rf_log2`

Performs cell-wise logarithm with base 2.

#### rf_log1p

_Python_: 

    Tile rf_log1p(Tile tile)

_SQL_: `rf_log1p`

Performs natural logarithm of cell values plus one. Inverse of @ref:[`rf_expm1`](reference.md#rf-expm1).

### Tile Statistics

The following functions compute a statistical summary per row of a `tile` column. The statistics are computed across the cells of a single `tile`, within each DataFrame Row.  Consider the following example.

```python
import pyspark.functions as F
spark.sql("""
 SELECT 1 as id, rf_make_ones_tile(5, 5, 'float32') as t 
 UNION
 SELECT 2 as id, rf_local_multiply(rf_tile_ones(5, 5, 'float32'), 3) as t 
 """).select(F.col('id'), rf_tile_sum(F.col('t'))).show()


+---+-----------+
| id|rf_tile_sum(t)|
+---+-----------+
|  2|       75.0|
|  1|       25.0|
+---+-----------+
```


#### rf_tile_sum

_Python_:

    Double rf_tile_sum(Tile tile)
    
_SQL_:  `rf_tile_sum`

Computes the sum of cells in each row of column `tile`, ignoring nodata values.

#### rf_tile_mean

_Python_:

    Double rf_tile_mean(Tile tile)
    
_SQL_:  `rf_tile_mean`

Computes the mean of cells in each row of column `tile`, ignoring nodata values.


#### rf_tile_min

_Python_:

    Double rf_tile_min(Tile tile)
    
_SQL_:  `rf_tile_min`

Computes the min of cells in each row of column `tile`, ignoring nodata values.


#### rf_tile_max

_Python_:

    Double rf_tile_max(Tile tile)
    
_SQL_:  `rf_tile_max`

Computes the max of cells in each row of column `tile`, ignoring nodata values.


#### rf_no_data_cells

_Python_:

    Long rf_no_data_cells(Tile tile)
    
_SQL_: `rf_no_data_cells`

Return the count of nodata cells in the `tile`.

#### rf_data_cells

_Python_:

    Long rf_data_cells(Tile tile)
    
_SQL_: `rf_data_cells`

Return the count of data cells in the `tile`.

#### rf_tile_stats

_Python_:

    Struct[Long, Long, Double, Double, Double, Double] rf_tile_stats(Tile tile)
    
_SQL_: `rf_tile_stats`

Computes the following statistics of cells in each row of column `tile`: data cell count, nodata cell count, minimum, maximum, mean, and variance. The minimum, maximum, mean, and variance are computed ignoring nodata values. 


#### rf_tile_histogram

_Python_:

    Struct[Struct[Long, Long, Double, Double, Double, Double], Array[Struct[Double, Long]]] rf_tile_histogram(Tile tile)
    
_SQL_:  `rf_rf_tile_histogram`

Computes a statistical summary of cell values within each row of `tile`. Resulting column has the below schema. Note that several of the other `tile` statistics functions are convenience methods to extract parts of this result. Related is the @ref:[`rf_agg_approx_histogram`](reference.md#rf-agg-approx-histogram) which computes the statistics across all rows in a group.

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
from pyrasterframes.functions import rf_agg_mean
rf.groupby(month(rf.datetime)).agg(rf_agg_mean(rf.red_band).alias('red_mean_monthly'))
```

Continuing our example from the @ref:[Tile Statistics](reference.md#tile-statistics) section, consider the following. Note that only a single row is returned. It is averaging 25 values of 1.0 and 25 values of 3.0, across the fifty cells in two rows.

```python 
spark.sql("""
SELECT 1 as id, rf_tile_ones(5, 5, 'float32') as t 
UNION
SELECT 2 as id, rf_local_multiply_scalar(rf_tile_ones(5, 5, 'float32'), 3) as t 
""").agg(rf_agg_mean(F.col('t'))).show(10, False)

+--------------+
|rf_agg_mean(t)|
+--------------+
|2.0           |
+--------------+
```

#### rf_agg_mean

_Python_:

    Double rf_agg_mean(Tile tile)
    
_SQL_: @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`(tile).mean`

Aggregates over the `tile` and return the mean of cell values, ignoring nodata. Equivalent to @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`.mean`.


#### rf_agg_data_cells

_Python_:

    Long rf_agg_data_cells(Tile tile)
    
_SQL_: @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`(tile).dataCells`

Aggregates over the `tile` and return the count of data cells. Equivalent to @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`.dataCells`. C.F. `data_cells`; equivalent code:

```python
rf.select(rf_agg_data_cells(rf.tile).alias('agg_data_cell')).show()
# Equivalent to
rf.agg(F.sum(rf_data_cells(rf.tile)).alias('agg_data_cell')).show()
```

#### rf_agg_no_data_cells

_Python_:

    Long rf_agg_no_data_cells(Tile tile)
    
_SQL_: @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`(tile).noDataCells`

Aggregates over the `tile` and return the count of nodata cells. Equivalent to @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`.noDataCells`. C.F. @ref:[`rf_no_data_cells`](reference.md#rf-no-data-cells) a row-wise count of no data cells.

#### rf_agg_stats

_Python_: 

    Struct[Long, Long, Double, Double, Double, Double] rf_agg_stats(Tile tile)
    
_SQL_:  `rf_agg_stats`

Aggregates over the `tile` and returns statistical summaries of cell values: number of data cells, number of nodata cells, minimum, maximum, mean, and variance. The minimum, maximum, mean, and variance ignore the presence of nodata. 

#### rf_agg_approx_histogram

_Python_:

    Struct[Struct[Long, Long, Double, Double, Double, Double], Array[Struct[Double, Long]]] rf_agg_approx_histogram(Tile tile)
    
_SQL_: `rf_agg_approx_histogram`

Aggregates over the `tile` return statistical summaries of the cell values, including a histogram, in the below schema. The `bins` array is of tuples of histogram values and counts. Typically values are plotted on the x-axis and counts on the y-axis. 

Note that several of the other cell value statistics functions are convenience methods to extract parts of this result. Related is the @ref:[`rf_tile_histogram`](reference.md#rf-tile-histogram) function which operates on a single row at a time.

```
 |-- agg_approx_histogram: struct (nullable = true)
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

Consider again our example for Tile Statistics and Aggregate Tile Statistics, this time apply @ref:[`rf_agg_local_mean`](reference.md#rf-agg-local-mean). We see that it is computing the element-wise mean across the two rows. In this case it is computing the mean of one value of 1.0 and one value of 3.0 to arrive at the element-wise mean, but doing so twenty-five times, one for each position in the `tile`.


```python
import pyspark.functions as F
lam = spark.sql("""
SELECT 1 as id, rf_tile_ones(5, 5, 'float32') as t 
UNION
SELECT 2 as id, rf_local_multiply(rf_tile_ones(5, 5, 'float32'), 3) as t 
""").agg(rf_local_agg_mean(F.col('t')).alias('l')) \

## local_agg_mean returns a tile
lam.select(rf_tile_dimensions(lam.l)).show()
## 
+------------------+
|tile_dimensions(l)|
+------------------+
|            [5, 5]|
+------------------+ 
##

lam.select(rf_explode_tiles(lam.l)).show(10, False)
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


#### rf_agg_local_max 

_Python_:

    Tile rf_agg_local_max(Tile tile)
    
_SQL_: `rf_agg_local_max`

Compute the cell-local maximum operation over Tiles in a column. 

#### rf_agg_local_min 

_Python_:

    Tile rf_agg_local_min(Tile tile)
    
_SQL_: `rf_agg_local_min`

Compute the cell-local minimum operation over Tiles in a column. 

#### rf_agg_local_mean 

_Python_:

    Tile rf_agg_local_mean(Tile tile)
    
_SQL_: `rf_agg_local_mean`

Compute the cell-local mean operation over Tiles in a column. 

#### rf_agg_local_data_cells 

_Python_:

    Tile rf_agg_local_data_cells(Tile tile)
    
_SQL_: `rf_agg_local_data_cells`

Compute the cell-local count of data cells over Tiles in a column. Returned `tile` has a cell type of `int32`.

#### rf_agg_local_no_data_cells

_Python_:

    Tile rf_agg_local_no_data_cells(Tile tile)
    
_SQL_: `rf_agg_local_no_data_cells`

Compute the cell-local count of nodata cells over Tiles in a column. Returned `tile` has a cell type of `int32`.

#### rf_agg_local_stats 

_Python_:

    Struct[Tile, Tile, Tile, Tile, Tile] rf_agg_local_stats(Tile tile)
    
_SQL_: `rf_agg_local_stats`

Compute cell-local aggregate count, minimum, maximum, mean, and variance for a column of Tiles. Returns a struct of five `tile`s.


### Converting Tiles 

RasterFrames provides several ways to convert a `tile` into other data structures. See also functions for @ref:[creating tiles](reference.md#tile-creation).

#### rf_explode_tiles

_Python_:

    Int, Int, Numeric* rf_explode_tiles(Tile* tile)
    
_SQL_: `rf_explode_tiles`

Create a row for each cell in `tile` columns. Many `tile` columns can be passed in, and the returned DataFrame will have one numeric column per input.  There will also be columns for `column_index` and `row_index`. Inverse of @ref:[`rf_assemble_tile`](reference.md#rf-assemble-tile). When using this function, be sure to have a unique identifier for rows in order to successfully invert the operation.

#### rf_explode_tiles_sample

_Python_:

    Int, Int, Numeric* rf_explode_tiles_sample(Double sample_frac, Long seed, Tile* tile)
    
Python only. As with @ref:[`rf_explode_tiles`](reference.md#rf-explode-tiles), but taking a randomly sampled subset of cells. Equivalent to the below, but this implementation is optimized for speed. Parameter `sample_frac` should be between 0.0 and 1.0. 

```python
df.select(df.id, rf_explode_tiles(df.tile1, df.tile2, df.tile3)) \
    .sample(False, 0.05, 8675309)
# Equivalent result, faster
df.select(df.id, rf_explode_tiles_sample(0.05, 8675309, df.tile1, df.tile2, df.tile3)) \
```

#### rf_tile_to_array_int

_Python_:

    Array rf_tile_to_array_int(Tile tile)
    
_SQL_: `rf_tile_to_array_int`


Convert Tile column to Spark SQL [Array](http://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType), in row-major order. Float cell types will be coerced to integral type by flooring.


#### rf_tile_to_array_double

_Python_:

    Array rf_tile_to_arry_double(Tile tile)
    
_SQL_: `rf_tile_to_array_double`

Convert tile column to Spark [Array](http://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType), in row-major order. Integral cell types will be coerced to floats.


#### rf_render_ascii

_Python_:

    String rf_render_ascii(Tile tile)

_SQL_:   `rf_render_ascii`

Pretty print the tile values as plain text.

#### rf_render_matrix

_Python_:

    String rf_render_matrix(Tile tile)

_SQL_:   `rf_render_matrix`

Render Tile cell values as numeric values, for debugging purposes.

[RasterFunctions]: org.locationtech.rasterframes.RasterFunctions
[scaladoc]: latest/api/index.html

