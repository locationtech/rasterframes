# Concepts

There are a number of Earth-observation (EO) concepts that crop up in the discussion of RasterFrames features. We'll cover these briefly in the sections below. However, here are a few links providing a more extensive introduction to working with Earth observation data.

* [_Fundamentals of Remote Sensing_](https://www.nrcan.gc.ca/maps-tools-and-publications/satellite-imagery-and-air-photos/tutorial-fundamentals-remote-sensing/9309)
* [_Newcomers Earth Observation Guide_](https://business.esa.int/newcomers-earth-observation-guide)
* [_Earth Observation Markets and Applications_](https://www.ofcom.org.uk/__data/assets/pdf_file/0021/82047/introduction_eo_for_ofcom_june_2015_no_video.pdf)

## Raster

A raster is a regular grid of numeric values. A raster can be thought of as an image, as is the case if the values in the grid represent brightness along a greyscale. More generally, a raster can measure many different phenomena or encode a variety of different discrete classifications.

## Cell

A cell is a single row and column intersection in the raster grid. It is a single pixel in an image. A cell's value often represents one sample from a sensor encoded as a scalar value associated with a specific location and time. 

## Cell Type

A numeric cell value may be encoded in a number of different computer numeric formats. There are typically three characteristics used to describe a cell type:

* word size (bit-width)
* signed vs unsigned
* integral vs floating-point


The most frequently encountered cell types in RasterFrames are below.

| Name | Abbreviation | Description | Range |
| --- | --- | --- | --- |
| Byte | `int8` | Signed 8-bit integral | -128 to 127 |
| Unsigned Byte | `uint8` | Unsigned 8-bit integral | 0 to 255 |
| Short | `int16` | Signed 16-bit integral | -32,768 to 32,767 |
| Unsigned Short | `uint16` | Unsigned 16-bit integral | 0 to 65,535 |
| Int | `int32` | Signed 32-bit integral | -2,147,483,648 to 2,147,483,647 |
| Unsigned Int | `uint32` | Unsigned 32-bit integral | 0 to 4,294,967,295 |
| Float | `float32` | 32-bit floating-point | -3.4028235E38 to 3.4028235E38 |
| Double | `float64` | 64-bit floating-point | -1.7976931348623157E308 to 1.7976931348623157E308 |

See the section on [“NoData” Handling](nodata-handling.md) for additional discussion on cell types and more exhaustive coverage of available cell types.

## NoData

A "NoData" (or N/A) value is a specifically identified value for a cell type used to indicate the absence of data. See the section on @ref:[“NoData” Handling](nodata-handling.md) for additional discussion on "NoData".

## Scene

A scene (or granule) is a discrete instance of EO @ref:[raster data](concepts.md#raster) with a specific extent (region), date-time, and map projection (or CRS).

## Band

A @ref:[scene](concepts.md#scene) frequently defines many different measurements captured at the same date-time, over the same extent, and meant to be processed together. These different measurements are referred to as bands. The name comes from the varying bandwidths of light and electromagnetic radiation measured in many EO datasets. 

## Coordinate Reference System (CRS)

A [coordinate reference system (or spatial reference system)][CRS] is a set of mathematical constructs used to translate locations on the three-dimensional surface of the earth to the two dimensional raster grid. A CRS typically accompanies any EO data so it can be precisely located. 

## Extent

An extent (or bounding box) is a rectangular region specifying the geospatial coverage of a @ref:[raster](concepts.md#raster) or @ref:[tile](concepts.md#tile), a two-dimensional array of @ref:[cells](concepts.md#cell) within a single CRS.

## Tile

A tile (sometimes called a "chip") is a rectangular subset of a @ref:[scene](concepts.md#scene). As a scene is a raster, a tile is also a raster. A tile can conceptually be thought of as a two-dimensional array. 

Some EO data has many @ref:[bands](concepts.md#band) or channels. Within RasterFrames, this third dimension is handled across columns of the DataFrame, such that the tiles within DataFrames are all two-dimensional arrays.

Tiles are often square and the dimensions are some power of two, for example 256 by 256.

The tile is the primary discretization unit used in RasterFrames. The scene's overall @ref:[extent](concepts.md#extent) is carved up into smaller extents and spread across rows. 

[CRS]: https://en.wikipedia.org/wiki/Spatial_reference_system
