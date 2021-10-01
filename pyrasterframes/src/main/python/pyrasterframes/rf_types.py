#
# This software is licensed under the Apache 2 license, quoted below.
#
# Copyright 2019 Astraea, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# [http://www.apache.org/licenses/LICENSE-2.0]
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# SPDX-License-Identifier: Apache-2.0
#

"""
This module contains all types relevant to PyRasterFrames. Classes in this module are
meant to provide smoother pathways between the jvm and Python, and whenever possible,
the implementations take advantage of the existing Scala functionality. The RasterFrameLayer
class here provides the PyRasterFrames entry point.
"""
from itertools import product
import functools, math

import pyproj
from pyspark import SparkContext
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import (UserDefinedType, StructType, StructField, BinaryType, DoubleType, ShortType, IntegerType, StringType)

from pyspark.ml.param.shared import HasInputCols
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

from pyrasterframes.rf_context import RFContext
from pyspark.sql import SparkSession
from py4j.java_collections import Sequence

import numpy as np

from typing import List, Tuple

__all__ = ['RasterFrameLayer', 'Tile', 'TileUDT', 'CellType', 'Extent',
           'CRS', 'CrsUDT', 'RasterSourceUDT', 'TileExploder', 'NoDataFilter']


class cached_property(object):
    def __init__(self, function):
        self.function = function
        functools.update_wrapper(self, function)

    def __get__(self, obj, type_):
        if obj is None:
            return self
        val = self.function(obj)
        obj.__dict__[self.function.__name__] = val
        return val

class RasterFrameLayer(DataFrame):
    def __init__(self, jdf: DataFrame, spark_session: SparkSession):
        DataFrame.__init__(self, jdf, spark_session._wrapped)
        self._jrfctx = spark_session.rasterframes._jrfctx

    def tile_columns(self) -> List[Column]:
        """
        Fetches columns of type Tile.
        :return: One or more Column instances associated with Tiles.
        """
        cols = self._jrfctx.tileColumns(self._jdf)
        return [Column(c) for c in cols]

    def spatial_key_column(self) -> Column:
        """
        Fetch the tagged spatial key column.
        :return: Spatial key column
        """
        col = self._jrfctx.spatialKeyColumn(self._jdf)
        return Column(col)

    def temporal_key_column(self) -> Column:
        """
        Fetch the temporal key column, if any.
        :return: Temporal key column, or None.
        """
        col = self._jrfctx.temporalKeyColumn(self._jdf)
        return col and Column(col)

    def tile_layer_metadata(self):
        """
        Fetch the tile layer metadata.
        :return: A dictionary of metadata.
        """
        import json
        return json.loads(str(self._jrfctx.tileLayerMetadata(self._jdf)))

    def spatial_join(self, other_df: DataFrame):
        """
        Spatially join this RasterFrameLayer to the given RasterFrameLayer.
        :return: Joined RasterFrameLayer.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.spatialJoin(self._jdf, other_df._jdf)
        return RasterFrameLayer(df, ctx._spark_session)

    def to_int_raster(self, colname: str, cols: int, rows: int) -> Sequence:
        """
        Convert a tile to an Int raster
        :return: array containing values of the tile's cells
        """
        resArr = self._jrfctx.toIntRaster(self._jdf, colname, cols, rows)
        return resArr

    def to_double_raster(self, colname: str, cols: int, rows: int) -> Sequence:
        """
        Convert a tile to an Double raster
        :return: array containing values of the tile's cells
        """
        resArr = self._jrfctx.toDoubleRaster(self._jdf, colname, cols, rows)
        return resArr

    def with_bounds(self):
        """
        Add a column called "bounds" containing the extent of each row.
        :return: RasterFrameLayer with "bounds" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withBounds(self._jdf)
        return RasterFrameLayer(df, ctx._spark_session)

    def with_center(self):
        """
        Add a column called "center" containing the center of the extent of each row.
        :return: RasterFrameLayer with "center" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withCenter(self._jdf)
        return RasterFrameLayer(df, ctx._spark_session)

    def with_center_lat_lng(self):
        """
        Add a column called "center" containing the center of the extent of each row in Lat Long form.
        :return: RasterFrameLayer with "center" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withCenterLatLng(self._jdf)
        return RasterFrameLayer(df, ctx._spark_session)

    def with_spatial_index(self):
        """
        Add a column containing the spatial index of each row.
        :return: RasterFrameLayer with "center" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withSpatialIndex(self._jdf)
        return RasterFrameLayer(df, ctx._spark_session)


class RasterSourceUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("raster_source_kryo", BinaryType(), False)])

    @classmethod
    def module(cls):
        return 'pyrasterframes.rf_types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.rf.RasterSourceUDT'

    def needConversion(self):
        return False

    # The contents of a RasterSource is opaque in the Python context.
    # Just pass data through unmodified.
    def serialize(self, obj):
        return obj

    def deserialize(self, datum):
        return datum


class Extent(object):
    def __init__(self, xmin: float, ymin: float, xmax: float, ymax: float):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

    @property
    def width(self):
        return math.fabs(self.xmax - self.xmin)

    @property
    def height(self):
        return math.fabs(self.ymax - self.ymin)

    @classmethod
    def from_row(cls, row):
        return Extent(row.xmin, row.ymin, row.xmax, row.ymax)

    @cached_property
    def __jvm__(self):
        return RFContext.jvm().geotrellis.vector.Extent(self.xmin, self.ymin, self.xmax, self.ymax)

    @classmethod
    def _from_jvm(self, obj):
        return Extent(obj.xmin(), obj.ymin(), obj.xmax(), obj.ymax())

    def reproject(self, src_crs, dest_crs):
        jvmret = RFContext.call("_reprojectExtent", self.__jvm__, src_crs, dest_crs)
        return Extent._from_jvm(jvmret)

    def buffer(self, amount):
        return Extent(
            self.xmin - amount,
            self.ymin - amount,
            self.xmax + amount,
            self.ymax + amount
        )

    def __str__(self):
        return self.__jvm__.toString()

class CRS(object):
    # NB: The name `crsProj4` has to match what's used in StandardSerializers.crsSerializers
    def __init__(self, crsProj4):
        if isinstance(crsProj4, pyproj.CRS):
            self.crsProj4 = crsProj4.to_proj4()
        elif isinstance(crsProj4, str):
            self.crsProj4 = crsProj4
        else:
            raise ValueError('Unexpected CRS definition type: {}'.format(type(crsProj4)))

    @cached_property
    def __jvm__(self):
        comp = RFContext.active().companion_of("org.locationtech.rasterframes.model.LazyCRS")
        return comp.apply(self.crsProj4)

    def __str__(self):
        return self.crsProj4

    @property
    def proj4_str(self):
        """Alias for `crsProj4`"""
        return self.crsProj4

    def __eq__(self, other):
        return isinstance(other, CRS) and self.crsProj4 == other.crsProj4


class CellType(object):
    def __init__(self, cell_type_name):
        assert(isinstance(cell_type_name, str))
        self.cell_type_name = cell_type_name

    @classmethod
    def from_numpy_dtype(cls, np_dtype: np.dtype):
        return CellType(str(np_dtype.name))

    @classmethod
    def bool(cls):
        return CellType('bool')

    @classmethod
    def int8(cls):
        return CellType('int8')

    @classmethod
    def uint8(cls):
        return CellType('uint8')

    @classmethod
    def int16(cls):
        return CellType('int16')

    @classmethod
    def uint16(cls):
        return CellType('uint16')

    @classmethod
    def int32(cls):
        return CellType('int32')

    @classmethod
    def float32(cls):
        return CellType('float32')

    @classmethod
    def float64(cls):
        return CellType('float64')

    def is_raw(self) -> bool:
        return self.cell_type_name.endswith('raw')

    def is_user_defined_no_data(self) -> bool:
        return "ud" in self.cell_type_name

    def is_default_no_data(self) -> bool:
        return not (self.is_raw() or self.is_user_defined_no_data())

    def is_floating_point(self) -> bool:
        return self.cell_type_name.startswith('float')

    def base_cell_type_name(self) -> str:
        if self.is_raw():
            return self.cell_type_name[:-3]
        elif self.is_user_defined_no_data():
            return self.cell_type_name.split('ud')[0]
        else:
            return self.cell_type_name

    def has_no_data(self) -> bool:
        return not self.is_raw()

    def no_data_value(self):
        if self.is_raw():
            return None
        elif self.is_user_defined_no_data():
            num_str = self.cell_type_name.split('ud')[1]
            if self.is_floating_point():
                return float(num_str)
            else:
                return int(num_str)
        else:
            if self.is_floating_point():
                return np.nan
            else:
                n = self.base_cell_type_name()
                if n == 'uint8' or n == 'uint16':
                    return 0
                elif n == 'int8':
                    return -128
                elif n == 'int16':
                    return -32768
                elif n == 'int32':
                    return -2147483648
                elif n == 'bool':
                    return None
        raise Exception("Unable to determine no_data_value from '{}'".format(n))

    def to_numpy_dtype(self) -> np.dtype:
        n = self.base_cell_type_name()
        return np.dtype(n).newbyteorder('>')

    def with_no_data_value(self, no_data):
        if self.has_no_data() and self.no_data_value() == no_data:
            return self
        if self.is_floating_point():
            no_data = str(float(no_data))
        else:
            no_data = str(int(no_data))
        return CellType(self.base_cell_type_name() + 'ud' + no_data)

    def __eq__(self, other):
        if type(other) is type(self):
            return self.cell_type_name == other.cell_type_name
        else:
            return False

    def __str__(self):
        return "CellType({}, {})".format(self.cell_type_name, self.no_data_value())

    def __repr__(self):
        return self.cell_type_name


class Tile(object):
    def __init__(self, cells, cell_type=None, grid_bounds=None):
        if cell_type is None:
            # infer cell type from the cells dtype and whether or not it is masked
            ct = CellType.from_numpy_dtype(cells.dtype)
            if isinstance(cells, np.ma.MaskedArray):
                ct = ct.with_no_data_value(cells.fill_value)
            self.cell_type = ct
        else:
            self.cell_type = cell_type
        self.cells = cells.astype(self.cell_type.to_numpy_dtype())

        if self.cell_type.has_no_data():
            nd_value = self.cell_type.no_data_value()
            if np.isnan(nd_value):
                self.cells = np.ma.masked_invalid(self.cells)
            else:
                # if the value in the array is `nd_value`, it is masked as nodata
                self.cells = np.ma.masked_equal(self.cells, nd_value)

        # is it a buffer tile? crop it on extraction to preserve the tile behavior
        if grid_bounds is not None:
            colmin, rowmin, colmax, rowmax = grid_bounds
            self.cells = self.cells[rowmin:(rowmax+1), colmin:(colmax+1)]

    def __eq__(self, other):
        if type(other) is type(self):
            return self.cell_type == other.cell_type and \
                   np.ma.allequal(self.cells, other.cells, fill_value=True)
        else:
            return False

    def __str__(self):
        return "Tile(dimensions={}, cell_type={}, cells=\n{})" \
            .format(self.dimensions(), self.cell_type, self.cells)

    def __repr__(self):
        return "Tile({}, {})" \
            .format(repr(self.cells), repr(self.cell_type))

    def __add__(self, right):
        if isinstance(right, Tile):
            other = right.cells
        else:
            other = right

        return Tile(np.add(self.cells, other))

    def __sub__(self, right):
        if isinstance(right, Tile):
            other = right.cells
        else:
            other = right
        return Tile(np.subtract(self.cells, other))

    def __mul__(self, right):
        if isinstance(right, Tile):
            other = right.cells
        else:
            other = right
        return Tile(np.multiply(self.cells, other))

    def __truediv__(self, right):
        if isinstance(right, Tile):
            other = right.cells
        else:
            other = right
        return Tile(np.true_divide(self.cells, other))

    def __div__(self, right):
        return self.__truediv__(right)

    def __matmul__(self, right):
        if isinstance(right, Tile):
            other = right.cells
        else:
            other = right
        return Tile(np.matmul(self.cells, other))

    def dimensions(self) -> Tuple[int, int]:
        """ Return a list of cols, rows as is conventional in GeoTrellis and RasterFrames."""
        return [self.cells.shape[1], self.cells.shape[0]]


class TileUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        """
        Mirrors `schema` in scala companion object org.apache.spark.sql.rf.TileUDT
        """
        extent = StructType([
            StructField("xmin",DoubleType(), True),
            StructField("ymin",DoubleType(), True),
            StructField("xmax",DoubleType(), True),
            StructField("ymax",DoubleType(), True)
        ])
        grid = StructType([
            StructField("colMin", IntegerType(), True),
            StructField("rowMin", IntegerType(), True),
            StructField("colMax", IntegerType(), True),
            StructField("rowMax", IntegerType() ,True)
        ])

        ref = StructType([
            StructField("source", StructType([
                StructField("raster_source_kryo", BinaryType(), False)
            ]),True),
            StructField("bandIndex", IntegerType(), True),
            StructField("subextent", extent ,True),
            StructField("subgrid", grid, True),
        ])

        return StructType([
             StructField("cellType", StringType(), False),
             StructField("cols", IntegerType(), False),
             StructField("rows", IntegerType(), False),
             StructField("cells", BinaryType(), True),
             StructField("gridBounds", grid, True),
             StructField("ref", ref, True)
        ])

    @classmethod
    def module(cls):
        return 'pyrasterframes.rf_types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.rf.TileUDT'

    def serialize(self, tile):
        cells = bytearray(tile.cells.flatten().tobytes())
        dims = tile.dimensions()
        return [
            tile.cell_type.cell_type_name,
            dims[0],
            dims[1],
            cells,
            None,
            None
        ]

    def deserialize(self, datum):
        """
        Convert catalyst representation of Tile to Python version. NB: This is expensive.
        :param datum:
        :return: A Tile object from row data.
        """

        cell_data_bytes = datum.cells
        if cell_data_bytes is None:
            if datum.ref is None:
                raise Exception("Invalid Tile structure. Missing cells and reference")
            else:
                payload = datum.ref
                ref = RFContext.active()._resolve_raster_ref(payload)
                cell_type = CellType(ref.cellType().name())
                cols = ref.cols()
                rows = ref.rows()
                cell_data_bytes = ref.tile().toBytes()
        else:
            cell_type = CellType(datum.cellType)
            cols = datum.cols
            rows = datum.rows

        if cell_data_bytes is None:
            raise Exception("Unable to fetch cell data from: " + repr(datum))

        try:
            as_numpy = np.frombuffer(cell_data_bytes, dtype=cell_type.to_numpy_dtype())
            reshaped = as_numpy.reshape((rows, cols))
            t = Tile(reshaped, cell_type, datum.gridBounds)
        except ValueError as e:
            raise ValueError({
                "cell_type": cell_type,
                "cols": cols,
                "rows": rows,
                "cell_data.length": len(cell_data_bytes),
                "cell_data.type": type(cell_data_bytes),
                "cell_data.values": repr(cell_data_bytes),
                "grid_bounds": datum.gridBounds
            }, e)
        return t

    deserialize.__safe_for_unpickling__ = True


Tile.__UDT__ = TileUDT()


class CrsUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        """
        Mirrors `schema` in scala companion object org.apache.spark.sql.rf.CrsUDT
        """
        return StringType()

    @classmethod
    def module(cls):
        return 'pyrasterframes.rf_types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.rf.CrsUDT'

    def serialize(self, crs):
        return crs.proj4_str

    def deserialize(self, datum):
        return CRS(datum)

    deserialize.__safe_for_unpickling__ = True


CRS.__UDT__ = CrsUDT()


class TileExploder(JavaTransformer, DefaultParamsReadable, DefaultParamsWritable):
    """
    Python wrapper for TileExploder.scala
    """

    def __init__(self):
        super(TileExploder, self).__init__()
        self._java_obj = self._new_java_obj("org.locationtech.rasterframes.ml.TileExploder", self.uid)


class NoDataFilter(JavaTransformer, HasInputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    Python wrapper for NoDataFilter.scala
    """

    def __init__(self):
        super(NoDataFilter, self).__init__()
        self._java_obj = self._new_java_obj("org.locationtech.rasterframes.ml.NoDataFilter", self.uid)


    def setInputCols(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCols=value)
