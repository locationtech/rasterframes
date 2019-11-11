# Function Reference

RasterFrames provides a rich set of columnar function for processing geospatial raster data. In Spark SQL, the functions are already registered in the SQL engine; they are usually prefixed with `rf_`. In Python, they are available in the `pyrasterframes.rasterfunctions` module.

The convention in this document will be to define the function signature as below, with its return type, the function name, and named arguments with their types.

```
ReturnDataType function_name(InputDataType argument1, InputDataType argument2)
```

For the Scala documentation on these functions, see @scaladoc[`RasterFunctions`][RasterFunctions]. The full Scala API documentation can be found [here][scaladoc].

```python imports, echo=False
import pyrasterframes
from pyrasterframes.utils import create_rf_spark_session
from pyrasterframes.rasterfunctions import *
from IPython.display import display
import os.path

spark = create_rf_spark_session()
```

## List of Available SQL and Python Functions

@@toc { depth=3 }


To import RasterFrames functions into the environment, import from `pyrasterframes.rasterfunctions`.

```python
from pyrasterframes.rasterfunctions import *
```

Functions starting with `rf_`, which are for raster, and `st_`, which are for vector geometry,
become available for use with DataFrames. You can view all of the available functions with the following.

```python, evaluate=False
[fn for fn in dir() if fn.startswith('rf_') or fn.startswith('st_')]
```

## Vector Operations

Various LocationTech GeoMesa user-defined functions (UDFs) dealing with `geomtery` type columns are provided in the SQL engine and within the `pyrasterframes.rasterfunctions` Python module. These are documented in the [LocationTech GeoMesa Spark SQL documentation](https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#). These functions are all prefixed with `st_`.

RasterFrames provides some additional functions for vector geometry operations.

### st_reproject

    Geometry st_reproject(Geometry geom, String origin_crs, String destination_crs)


Reproject the vector `geom` from `origin_crs` to `destination_crs`. Both `_crs` arguments are either [proj4](https://proj4.org/usage/quickstart.html) strings, [EPSG codes](https://www.epsg-registry.org/) or [OGC WKT](https://www.opengeospatial.org/standards/wkt-crs) for coordinate reference systems.


### st_extent

    Struct[Double xmin, Double xmax, Double ymin, Double ymax] st_extent(Geometry geom)

Extracts the bounding box (extent/envelope) of the geometry.

See also GeoMesa [st_envelope](https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#st-envelope) which returns a Geometry type.

### st_geometry

    Geometry st_geometry(Struct[Double xmin, Double xmax, Double ymin, Double ymax] extent)

Convert an extent to a Geometry. The extent likely comes from @ref:[`st_extent`](reference.md#st-extent) or @ref:[`rf_extent`](reference.md#rf-extent).


### rf_spatial_index

    Long rf_spatial_index(Geometry geom, CRS crs)
    Long rf_spatial_index(Extent extent, CRS crs)
    Long rf_spatial_index(ProjectedRasterTile proj_raster, CRS crs)
    
Constructs a XZ2 index in WGS84/EPSG:4326 from either a Geometry, Extent, ProjectedRasterTile and its CRS. This function is useful for [range partitioning](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=registerjava#pyspark.sql.DataFrame.repartitionByRange).

## Tile Metadata and Mutation

Functions to access and change the particulars of a `tile`: its shape and the data type of its cells. See section on @ref:["NoData" handling](nodata-handling.md) for additional discussion of cell types.

### rf_dimensions

    Struct[Int, Int] rf_dimensions(Tile tile)

Get number of columns and rows in the `tile`, as a Struct of `cols` and `rows`.

### rf_cell_type

    Struct[String] rf_cell_type(Tile tile)

Get the cell type of the `tile`. The cell type can be changed with @ref:[rf_convert_cell_type](reference.md#rf-convert-cell-type).

### rf_tile

    Tile rf_tile(ProjectedRasterTile proj_raster)

Get the fully realized (non-lazy) `tile` from a `ProjectedRasterTile` struct column.

### rf_extent

    Struct[Double xmin, Double xmax, Double ymin, Double ymax] rf_extent(ProjectedRasterTile proj_raster)
    Struct[Double xmin, Double xmax, Double ymin, Double ymax] rf_extent(RasterSource proj_raster)

Fetches the extent (bounding box or envelope) of a `ProjectedRasterTile` or `RasterSource` type tile columns.

### rf_crs

    Struct rf_crs(ProjectedRasterTile proj_raster)
    Struct rf_crs(RasterSource proj_raster)
    Struct rf_crs(String crs_spec)

Fetch CRS structure representing the coordinate reference system of a `ProjectedRasterTile` or `RasterSource` type tile columns, or from a column of strings in the form supported by @ref:[`rf_mk_crs`](reference.md#rf-mk-crs). 

### rf_mk_crs

    Struct rf_mk_crs(String crsText)   

Construct a CRS structure from one of its string representations. Three forms are supported:

* [EPSG code](https://www.epsg-registry.org/): `EPSG:<integer>`
* [Proj4 string](https://proj.org/): `+proj <proj4 parameters>`
* [WKT String](http://www.geoapi.org/3.0/javadoc/org/opengis/referencing/doc-files/WKT.html) with embedded EPSG code: `GEOGCS["<name>", <datum>, <prime meridian>, <angular unit> {,<twin axes>} {,<authority>}]` 

Example: `SELECT rf_mk_crs('EPSG:4326')`

### rf_convert_cell_type

    Tile rf_convert_cell_type(Tile tile_col, CellType cell_type)
    Tile rf_convert_cell_type(Tile tile_col, String cell_type)

Convert `tile_col` to a different cell type. In Python you can pass a CellType object to `cell_type`.

### rf_interpret_cell_type_as

    Tile rf_interpret_cell_type_as(Tile tile_col, CellType cell_type)
    Tile rf_interpret_cell_type_as(Tile tile_col, String cell_type)

Change the interpretation of the `tile_col`'s cell values according to specified `cell_type`. In Python you can pass a CellType object to `cell_type`.

### rf_resample

    Tile rf_resample(Tile tile, Double factor)
    Tile rf_resample(Tile tile, Int factor)
    Tile rf_resample(Tile tile, Tile shape_tile)


Change the tile dimension. Passing a numeric `factor` will scale the number of columns and rows in the tile: 1.0 is the same number of columns and row; less than one downsamples the tile; and greater than one upsamples the tile. Passing a `shape_tile` as the second argument outputs `tile` having the same number of columns and rows as `shape_tile`. All resampling is by nearest neighbor method.

## Tile Creation

Functions to create a new Tile column, either from scratch or from existing data not yet in a `tile`.

### rf_make_zeros_tile

```
Tile rf_make_zeros_tile(Int tile_columns, Int tile_rows, [CellType cell_type])
Tile rf_make_zeros_tile(Int tile_columns, Int tile_rows, [String cell_type_name])
```

Create a `tile` of shape `tile_columns` by `tile_rows` full of zeros, with the optional cell type; default is float64. See @ref:[this discussion](nodata-handling.md#cell-types) on cell types for info on the `cell_type` argument. All arguments are literal values and not column expressions.

### rf_make_ones_tile

```
Tile rf_make_ones_tile(Int tile_columns, Int tile_rows, [CellType cell_type])
Tile rf_make_ones_tile(Int tile_columns, Int tile_rows, [String cell_type_name])
```


Create a `tile` of shape `tile_columns` by `tile_rows` full of ones, with the optional cell type; default is float64. See @ref:[this discussion](nodata-handling.md#cell-types) on cell types for info on the `cell_type` argument. All arguments are literal values and not column expressions.

### rf_make_constant_tile

    Tile rf_make_constant_tile(Numeric constant, Int tile_columns, Int tile_rows,  [CellType cell_type])
    Tile rf_make_constant_tile(Numeric constant, Int tile_columns, Int tile_rows,  [String cell_type_name])


Create a `tile` of shape `tile_columns` by `tile_rows` full of `constant`, with the optional cell type; default is float64. See @ref:[this discussion](nodata-handling.md#cell-types) on cell types for info on the `cell_type` argument. All arguments are literal values and not column expressions.


### rf_rasterize

    Tile rf_rasterize(Geometry geom, Geometry tile_bounds, Int value, Int tile_columns, Int tile_rows)


Convert a vector Geometry `geom` into a Tile representation. The `value` will be "burned-in" to the returned `tile` where the `geom` intersects the `tile_bounds`. Returned `tile` will have shape `tile_columns` by `tile_rows`. Values outside the `geom` will be assigned a NoData value. Returned `tile` has cell type `int32`, note that `value` is of type Int.

Parameters `tile_columns` and `tile_rows` are literals, not column expressions. The others are column expressions.

### rf_array_to_tile

    Tile rf_array_to_tile(Array arrayCol, Int numCols, Int numRows)

Python only. Create a `tile` from a Spark SQL [Array][Array], filling values in row-major order.

### rf_assemble_tile

    Tile rf_assemble_tile(Int colIndex, Int rowIndex, Numeric cellData, Int numCols, Int numRows, [CellType cell_type])
    Tile rf_assemble_tile(Int colIndex, Int rowIndex, Numeric cellData, Int numCols, Int numRows, [String cell_type_name])

SQL:  `Tile rf_assemble_tile(Int colIndex, Int rowIndex, Numeric cellData, Int numCols, Int numRows)`

Create `tile`s of dimension `numCols` by `numRows` from  a column of cell data with location indices. This function is the inverse of @ref:[`rf_explode_tiles`](reference.md#rf-explode-tiles). Intended use is with a `groupby`, producing one row with a new `tile` per group.  In Python, the `numCols`, `numRows` and `cellType` arguments are literal values, others are column expressions. 
See @ref:[this discussion](nodata-handling.md#cell-types) on cell types for info on the optional `cell_type` argument. The default is float64.
SQL implementation does not accept a cell_type argument. It returns a float64 cell type `tile` by default.

## Masking and NoData

See the @ref:[masking](masking.md) page for conceptual discussion of masking operations.

There are statistical functions of the count of data and NoData values per `tile` and aggregate over a `tile` column: @ref:[`rf_data_cells`](reference.md#rf-data-cells), @ref:[`rf_no_data_cells`](reference.md#rf-no-data-cells), @ref:[`rf_agg_data_cells`](reference.md#rf-agg-data-cells), and @ref:[`rf_agg_no_data_cells`](reference.md#rf-agg-no-data-cells).

Masking is a raster operation that sets specific cells to NoData based on the values in another raster.

### rf_mask

    Tile rf_mask(Tile tile, Tile mask, bool inverse)

Where the `mask` contains NoData, replace values in the `tile` with NoData.

Returned `tile` cell type will be coerced to one supporting NoData if it does not already.

`inverse` is a literal not a Column. If `inverse` is true, return the `tile` with NoData in locations where the `mask` _does not_ contain NoData. Equivalent to @ref:[`rf_inverse_mask`](reference.md#rf-inverse-mask).

See also @ref:[`rf_rasterize`](reference.md#rf-rasterize).

### rf_mask_by_value

    Tile rf_mask_by_value(Tile data_tile, Tile mask_tile, Int mask_value, bool inverse)

Generate a `tile` with the values from `data_tile`, with NoData in cells where the `mask_tile` is equal to `mask_value`.

`inverse` is a literal not a Column. If `inverse` is true, return the `data_tile` with NoData in locations where the `mask_tile` value is _not equal_ to `mask_value`. Equivalent to @ref:[`rf_inverse_mask_by_value`](reference.md#rf-inverse-mask-by-value).

### rf_mask_by_values

    Tile rf_mask_by_values(Tile data_tile, Tile mask_tile, Array mask_values)
    Tile rf_mask_by_values(Tile data_tile, Tile mask_tile, seq mask_values)

Generate a `tile` with the values from `data_tile`, with NoData in cells where the `mask_tile` is in the `mask_values` Array or list. `mask_values` can be a [`pyspark.sql.ArrayType`][Array] or a `list`.  

### rf_inverse_mask

    Tile rf_inverse_mask(Tile tile, Tile mask)

Where the `mask` _does not_ contain NoData, replace values in `tile` with NoData.


### rf_inverse_mask_by_value

    Tile rf_inverse_mask_by_value(Tile data_tile, Tile mask_tile, Int mask_value)

Generate a `tile` with the values from `data_tile`, with NoData in cells where the `mask_tile` is not equal to `mask_value`. In other words, only keep `data_tile` cells in locations where the `mask_tile` is equal to `mask_value`.

### rf_is_no_data_tile

    Boolean rf_is_no_data_tile(Tile)

Returns true if `tile` contains only NoData. By definition returns false if cell type does not support NoData. To count NoData cells or data cells, see @ref:[`rf_no_data_cells`](reference.md#rf-no-data-cells), @ref:[`rf_data_cells`](reference.md#rf-data-cells), @ref:[`rf_agg_no_data_cells`](reference.md#rf-agg-no-data-cells), @ref:[`rf_agg_data_cells`](reference.md#rf-agg-data-cells), @ref:[`rf_agg_local_no_data_cells`](reference.md#rf-agg-local-no-data-cells), and @ref:[`rf_agg_local_data_cells`](reference.md#rf-agg-local-data-cells). This function is distinguished from @ref:[`rf_for_all`](reference.md#rf-for-all), which tests that values are not NoData and nonzero.

### rf_local_no_data

    Tile rf_local_no_data(Tile tile)

Returns a tile with values of 1 in each cell where the input tile contains NoData. Otherwise values are 0.

### rf_local_data

    Tile rf_local_no_data(Tile tile)

Returns a tile with values of 0 in each cell where the input tile contains NoData. Otherwise values are 1.

### rf_local_data

### rf_with_no_data

    Tile rf_with_no_data(Tile tile, Double no_data_value)

Python only. Return a `tile` column marking as NoData all cells equal to `no_data_value`.

The `no_data_value` argument is a literal Double, not a Column expression.

If input `tile` had a NoData value already, the behaviour depends on if its cell type is floating point or not. For floating point cell type `tile`, NoData values on the input `tile` remain NoData values on the output. For integral cell type `tile`s, the previous NoData values become literal values.

## Local Map Algebra

[Local map algebra](https://gisgeography.com/map-algebra-global-zonal-focal-local/) raster operations are element-wise operations on a single tile (unary), between a `tile` and a scalar, between two `tile`s, or across many `tile`s.

When these operations encounter a NoData value in either operand, the cell in the resulting `tile` will have a NoData.

The binary local map algebra functions have similar variations in the Python API depending on the left hand side type:

 - `rf_local_op`: applies `op` to two columns; the right hand side can be a `tile` or a numeric column.
 - `rf_local_op_double`: applies `op` to a `tile` and a literal scalar, coercing the `tile` to a floating point type
 - `rf_local_op_int`: applies `op` to a `tile` and a literal scalar, without coercing the `tile` to a floating point type

The SQL API does not require the `rf_local_op_double` or `rf_local_op_int` forms (just `rf_local_op`).

Local map algebra operations for more than two `tile`s are implemented to work across rows in the DataFrame. As such, they are @ref:[aggregate functions](reference.md#tile-local-aggregate-statistics).

### rf_local_add

    Tile rf_local_add(Tile tile1, Tile rhs)
    Tile rf_local_add_int(Tile tile1, Int rhs)
    Tile rf_local_add_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise sum of `tile1` and `rhs`.

### rf_local_subtract

    Tile rf_local_subtract(Tile tile1, Tile rhs)
    Tile rf_local_subtract_int(Tile tile1, Int rhs)
    Tile rf_local_subtract_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise difference of `tile1` and `rhs`.


### rf_local_multiply

    Tile rf_local_multiply(Tile tile1, Tile rhs)
    Tile rf_local_multiply_int(Tile tile1, Int rhs)
    Tile rf_local_multiply_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise product of `tile1` and `rhs`. This is **not** the matrix multiplication of `tile1` and `rhs`.


### rf_local_divide

    Tile rf_local_divide(Tile tile1, Tile rhs)
    Tile rf_local_divide_int(Tile tile1, Int rhs)
    Tile rf_local_divide_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise quotient of `tile1` and `rhs`.


### rf_normalized_difference

    Tile rf_normalized_difference(Tile tile1, Tile tile2)


Compute the normalized difference of the the two `tile`s: `(tile1 - tile2) / (tile1 + tile2)`. Result is always floating point cell type. This function has no scalar variant.

### rf_local_less

    Tile rf_local_less(Tile tile1, Tile rhs)
    Tile rf_local_less_int(Tile tile1, Int rhs)
    Tile rf_local_less_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise evaluation of `tile1` is less than `rhs`.

### rf_local_less_equal

    Tile rf_local_less_equal(Tile tile1, Tile rhs)
    Tile rf_local_less_equal_int(Tile tile1, Int rhs)
    Tile rf_local_less_equal_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise evaluation of `tile1` is less than or equal to `rhs`.

### rf_local_greater

    Tile rf_local_greater(Tile tile1, Tile rhs)
    Tile rf_local_greater_int(Tile tile1, Int rhs)
    Tile rf_local_greater_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise evaluation of `tile1` is greater than `rhs`.

### rf_local_greater_equal

    Tile rf_local_greater_equal(Tile tile1, Tile rhs)
    Tile rf_local_greater_equal_int(Tile tile1, Int rhs)
    Tile rf_local_greater_equal_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise evaluation of `tile1` is greater than or equal to `rhs`.

### rf_local_equal

    Tile rf_local_equal(Tile tile1, Tile rhs)
    Tile rf_local_equal_int(Tile tile1, Int rhs)
    Tile rf_local_equal_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise equality of `tile1` and `rhs`.

### rf_local_unequal

    Tile rf_local_unequal(Tile tile1, Tile rhs)
    Tile rf_local_unequal_int(Tile tile1, Int rhs)
    Tile rf_local_unequal_double(Tile tile1, Double rhs)


Returns a `tile` column containing the element-wise inequality of `tile1` and `rhs`.

### rf_local_is_in

    Tile rf_local_is_in(Tile tile, Array array)
    Tile rf_local_is_in(Tile tile, list l)

Returns a `tile` column with cell values of 1 where the `tile` cell value is in the provided array or list. The `array` is a Spark SQL [Array][Array]. A python `list` of numeric values can also be passed.

### rf_round

    Tile rf_round(Tile tile)

Round cell values to the nearest integer without changing the cell type.

### rf_abs

    Tile rf_abs(Tile tile)

 Compute the absolute value of cell value.

### rf_exp

    Tile rf_exp(Tile tile)

Performs cell-wise exponential.

### rf_exp10

    Tile rf_exp10(Tile tile)

Compute 10 to the power of cell values.

### rf_exp2

    Tile rf_exp2(Tile tile)

Compute 2 to the power of cell values.

### rf_expm1

    Tile rf_expm1(Tile tile)

Performs cell-wise exponential, then subtract one. Inverse of @ref:[`log1p`](reference.md#log1p).

### rf_log

    Tile rf_log(Tile tile)

Performs cell-wise natural logarithm.

### rf_log10

    Tile rf_log10(Tile tile)

Performs cell-wise logarithm with base 10.

### rf_log2

    Tile rf_log2(Tile tile)

Performs cell-wise logarithm with base 2.

### rf_log1p

    Tile rf_log1p(Tile tile)

Performs natural logarithm of cell values plus one. Inverse of @ref:[`rf_expm1`](reference.md#rf-expm1).

## Tile Statistics

The following functions compute a statistical summary per row of a `tile` column. The statistics are computed across the cells of a single `tile`, within each DataFrame Row.

### rf_tile_sum

    Double rf_tile_sum(Tile tile)


Computes the sum of cells in each row of column `tile`, ignoring NoData values.

### rf_tile_mean

    Double rf_tile_mean(Tile tile)


Computes the mean of cells in each row of column `tile`, ignoring NoData values.


### rf_tile_min

    Double rf_tile_min(Tile tile)


Computes the min of cells in each row of column `tile`, ignoring NoData values.


### rf_tile_max

    Double rf_tile_max(Tile tile)


Computes the max of cells in each row of column `tile`, ignoring NoData values.


### rf_no_data_cells

    Long rf_no_data_cells(Tile tile)


Return the count of NoData cells in the `tile`.

### rf_data_cells

    Long rf_data_cells(Tile tile)

Return the count of data cells in the `tile`.

### rf_exists

    Boolean rf_exists(Tile tile)

Returns true if any cells in the tile are true (non-zero and not NoData).

### rf_for_all

    Boolean rf_for_all(Tile tile)

Returns true if all cells in the tile are true (non-zero and not NoData). See also @ref:[`rf_is_no_data_tile](reference.md#rf-is-no-data-tile), which tests that all cells are NoData.

### rf_tile_stats

    Struct[Long, Long, Double, Double, Double, Double] rf_tile_stats(Tile tile)

Computes the following statistics of cells in each row of column `tile`: data cell count, NoData cell count, minimum, maximum, mean, and variance. The minimum, maximum, mean, and variance are computed ignoring NoData values. Resulting column has the below schema.

```python echo=False
spark.sql("SELECT rf_tile_stats(rf_make_ones_tile(5, 5, 'float32')) as tile_stats").printSchema()
```

### rf_tile_histogram

    Struct[Array[Struct[Double, Long]]] rf_tile_histogram(Tile tile)

Computes a count of cell values within each row of `tile`. The `bins` array is of tuples of histogram values and counts. Typically values are plotted on the x-axis and counts on the y-axis. Resulting column has the below schema. Related is the @ref:[`rf_agg_approx_histogram`](reference.md#rf-agg-approx-histogram) which computes the statistics across all rows in a group.

```python echo=False
spark.sql("SELECT rf_tile_histogram(rf_make_ones_tile(5, 5, 'float32')) as tile_histogram").printSchema()
```

## Aggregate Tile Statistics

These functions compute statistical summaries over all of the cell values *and* across all the rows in the DataFrame or group.

### rf_agg_mean

    Double rf_agg_mean(Tile tile)

_SQL_: @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`(tile).mean`

Aggregates over the `tile` and return the mean of cell values, ignoring NoData. Equivalent to @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`.mean`.


### rf_agg_data_cells

    Long rf_agg_data_cells(Tile tile)

_SQL_: @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`(tile).dataCells`

Aggregates over the `tile` and return the count of data cells. Equivalent to @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`.dataCells`.

### rf_agg_no_data_cells

    Long rf_agg_no_data_cells(Tile tile)

_SQL_: @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`(tile).dataCells`

Aggregates over the `tile` and return the count of NoData cells. Equivalent to @ref:[`rf_agg_stats`](reference.md#rf-agg-stats)`.noDataCells`. C.F. @ref:[`rf_no_data_cells`](reference.md#rf-no-data-cells) a row-wise count of no data cells.

### rf_agg_stats

    Struct[Long, Long, Double, Double, Double, Double] rf_agg_stats(Tile tile)


Aggregates over the `tile` and returns statistical summaries of cell values: number of data cells, number of NoData cells, minimum, maximum, mean, and variance. The minimum, maximum, mean, and variance ignore the presence of NoData.

### rf_agg_approx_histogram

    Struct[Array[Struct[Double, Long]]] rf_agg_approx_histogram(Tile tile)


Aggregates over all of the rows in DataFrame of `tile` and returns a count of each cell value to create a histogram with values are plotted on the x-axis and counts on the y-axis. Related is the @ref:[`rf_tile_histogram`](reference.md#rf-tile-histogram) function which operates on a single row at a time.


## Tile Local Aggregate Statistics

Local statistics compute the element-wise statistics across a DataFrame or group of `tile`s, resulting in a `tile` that has the same dimension.

When these functions encounter NoData in a cell location, it will be ignored. 

### rf_agg_local_max

    Tile rf_agg_local_max(Tile tile)

Compute the cell-local maximum operation over `tile`s in a column.

### rf_agg_local_min

    Tile rf_agg_local_min(Tile tile)

Compute the cell-local minimum operation over `tile`s in a column.

### rf_agg_local_mean

    Tile rf_agg_local_mean(Tile tile)

Compute the cell-local mean operation over `tile`s in a column.

### rf_agg_local_data_cells

    Tile rf_agg_local_data_cells(Tile tile)

Compute the cell-local count of data cells over `tile`s in a column. Returned `tile` has a cell type of `int32`.

### rf_agg_local_no_data_cells

    Tile rf_agg_local_no_data_cells(Tile tile)

Compute the cell-local count of NoData cells over `tile`s in a column. Returned `tile` has a cell type of `int32`.

### rf_agg_local_stats

    Struct[Tile, Tile, Tile, Tile, Tile] rf_agg_local_stats(Tile tile)

Compute cell-local aggregate count, minimum, maximum, mean, and variance for a column of `tile`s. Returns a struct of five `tile`s.


## Converting Tiles

RasterFrames provides several ways to convert a `tile` into other data structures. See also functions for @ref:[creating tiles](reference.md#tile-creation).

### rf_explode_tiles

    Int, Int, Numeric* rf_explode_tiles(Tile* tile)

Create a row for each cell in `tile` columns. Many `tile` columns can be passed in, and the returned DataFrame will have one numeric column per input.  There will also be columns for `column_index` and `row_index`. Inverse of @ref:[`rf_assemble_tile`](reference.md#rf-assemble-tile). When using this function, be sure to have a unique identifier for rows in order to successfully invert the operation.

### rf_explode_tiles_sample

    Int, Int, Numeric* rf_explode_tiles_sample(Double sample_frac, Long seed, Tile* tile)

Python only. As with @ref:[`rf_explode_tiles`](reference.md#rf-explode-tiles), but taking a randomly sampled subset of cells. Equivalent to the `rf_explode-tiles`, but allows a random subset of the data to be selected. Parameter `sample_frac` should be between 0.0 and 1.0.

### rf_tile_to_array_int

    Array rf_tile_to_array_int(Tile tile)

Convert Tile column to Spark SQL [Array][Array], in row-major order. Float cell types will be coerced to integral type by flooring.

### rf_tile_to_array_double

    Array rf_tile_to_arry_double(Tile tile)

Convert tile column to Spark [Array][Array], in row-major order. Integral cell types will be coerced to floats.

### rf_render_ascii

    String rf_render_ascii(Tile tile)

Pretty print the tile values as plain text.

### rf_render_matrix

    String rf_render_matrix(Tile tile)

Render Tile cell values as numeric values, for debugging purposes.


### rf_rgb_composite

    Tile rf_rgb_composite(Tile red, Tile green, Tile blue)
    
Merges three bands into a single byte-packed RGB composite. It first scales each cell to fit into an unsigned byte, in the range 0-255, and then merges all three channels to fit into a 32-bit unsigned integer. This is useful when you want an RGB tile to render or to process with other color imagery tools. 


### rf_render_png

    Array rf_render_png(Tile red, Tile green, Tile blue)
    
Runs [`rf_rgb_composite`](reference.md#rf-rgb-composite) on the given tile columns and then encodes the result as a PNG byte array.

[RasterFunctions]: org.locationtech.rasterframes.RasterFunctions
[scaladoc]: latest/api/index.html
[Array]: http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.ArrayType