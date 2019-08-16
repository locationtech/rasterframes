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

from pyspark import SparkContext
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import (UserDefinedType, StructType, StructField, BinaryType, DoubleType, ShortType, IntegerType, StringType)

from pyspark.ml.param.shared import HasInputCols
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.util import JavaMLReadable, JavaMLWritable

from pyrasterframes.rf_context import RFContext

import numpy as np

__all__ = ['RasterFrameLayer', 'Tile', 'TileUDT', 'CellType', 'RasterSourceUDT', 'TileExploder', 'NoDataFilter']


class RasterFrameLayer(DataFrame):
    def __init__(self, jdf, spark_session):
        DataFrame.__init__(self, jdf, spark_session._wrapped)
        self._jrfctx = spark_session.rasterframes._jrfctx

    def tile_columns(self):
        """
        Fetches columns of type Tile.
        :return: One or more Column instances associated with Tiles.
        """
        cols = self._jrfctx.tileColumns(self._jdf)
        return [Column(c) for c in cols]

    def spatial_key_column(self):
        """
        Fetch the tagged spatial key column.
        :return: Spatial key column
        """
        col = self._jrfctx.spatialKeyColumn(self._jdf)
        return Column(col)

    def temporal_key_column(self):
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

    def spatial_join(self, other_df):
        """
        Spatially join this RasterFrameLayer to the given RasterFrameLayer.
        :return: Joined RasterFrameLayer.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.spatialJoin(self._jdf, other_df._jdf)
        return RasterFrameLayer(df, ctx._spark_session)

    def to_int_raster(self, colname, cols, rows):
        """
        Convert a tile to an Int raster
        :return: array containing values of the tile's cells
        """
        resArr = self._jrfctx.toIntRaster(self._jdf, colname, cols, rows)
        return resArr

    def to_double_raster(self, colname, cols, rows):
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


class CellType(object):
    def __init__(self, cell_type_name):
        self.cell_type_name = cell_type_name

    @classmethod
    def from_numpy_dtype(cls, np_dtype):
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

    def is_raw(self):
        return self.cell_type_name.endswith('raw')

    def is_user_defined_no_data(self):
        return "ud" in self.cell_type_name

    def is_default_no_data(self):
        return not (self.is_raw() or self.is_user_defined_no_data())

    def is_floating_point(self):
        return self.cell_type_name.startswith('float')

    def base_cell_type_name(self):
        if self.is_raw():
            return self.cell_type_name[:-3]
        elif self.is_user_defined_no_data():
            return self.cell_type_name.split('ud')[0]
        else:
            return self.cell_type_name

    def has_no_data(self):
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

    def to_numpy_dtype(self):
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
    def __init__(self, cells, cell_type=None):
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

    def dimensions(self):
        """ Return a list of cols, rows as is conventional in GeoTrellis and RasterFrames."""
        return [self.cells.shape[1], self.cells.shape[0]]

    def _repr_png_(self):
        """Provide default PNG rendering in IPython and Jupyter"""
        from pyrasterframes.rf_ipython import tile_to_png
        return tile_to_png(self)

    def show(self, lower_percentile=1, upper_percentile=99, axis=None, **imshow_args):
            """
            Display an image of the tile

            Parameters
            ----------
            lower_percentile: between 0 and 100 inclusive.
                              Specifies to clip values below this percentile
            upper_percentile: between 0 and 100 inclusive.
                              Specifies to clip values above this percentile
            axis : matplotlib axis object to plot onto. Creates new axis if None
            imshow_args : parameters to pass into matplotlib.pyplot.imshow
                          see https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.imshow.html
            Returns
            -------
            created or modified axis object
            """

            if axis is None:
                import matplotlib.pyplot as plt
                axis = plt.gca()

            arr = self.cells

            def normalize_cells(cells, lower_percentile=lower_percentile, upper_percentile=upper_percentile):
                assert upper_percentile > lower_percentile, 'invalid upper and lower percentiles'
                lower = np.percentile(cells, lower_percentile)
                upper = np.percentile(cells, upper_percentile)
                cells_clipped = np.clip(cells, lower, upper)
                return (cells_clipped - lower) / (upper - lower)

            axis.set_aspect('equal')
            axis.xaxis.set_ticks([])
            axis.yaxis.set_ticks([])

            axis.imshow(normalize_cells(arr), **imshow_args)

            return axis




class TileUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        """
        Mirrors `schema` in scala companion object org.apache.spark.sql.rf.TileUDT
        """
        return StructType([
            StructField("cell_context", StructType([
                StructField("cellType", StructType([
                    StructField("cellTypeName", StringType(), False)
                ]), False),
                StructField("dimensions", StructType([
                    StructField("cols", ShortType(), False),
                    StructField("rows", ShortType(), False)
                ]), False),
            ]), False),
            StructField("cell_data", StructType([
                StructField("cells", BinaryType(), True),
                StructField("ref", StructType([
                    StructField("source", RasterSourceUDT(), False),
                    StructField("bandIndex", IntegerType(), False),
                    StructField("subextent", StructType([
                        StructField("xmin", DoubleType(), False),
                        StructField("ymin", DoubleType(), False),
                        StructField("xmax", DoubleType(), False),
                        StructField("ymax", DoubleType(), False)
                    ]), True)
                ]), True)
            ]), False)
        ])

    @classmethod
    def module(cls):
        return 'pyrasterframes.rf_types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.rf.TileUDT'

    def serialize(self, tile):
        cells = bytearray(tile.cells.flatten().tobytes())
        row = [
            # cell_context
            [
                [tile.cell_type.cell_type_name],
                tile.dimensions()
            ],
            # cell_data
            [
                # cells
                cells,
                None
            ]
        ]
        return row

    def deserialize(self, datum):
        """
        Convert catalyst representation of Tile to Python version. NB: This is expensive.
        :param datum:
        :return: A Tile object from row data.
        """
        cell_type = CellType(datum.cell_context.cellType.cellTypeName)
        cols = datum.cell_context.dimensions.cols
        rows = datum.cell_context.dimensions.rows
        cell_data_bytes = datum.cell_data.cells
        if cell_data_bytes is None:
            if datum.cell_data.ref is None:
                raise Exception("Invalid Tile structure. Missing cells and reference")
            else:
                payload = datum.cell_data.ref
                cell_data_bytes = RFContext.active()._resolve_raster_ref(payload)

        if cell_data_bytes is None:
            raise Exception("Unable to fetch cell data from: " + repr(datum))

        try:
            as_numpy = np.frombuffer(cell_data_bytes, dtype=cell_type.to_numpy_dtype())
            reshaped = as_numpy.reshape((rows, cols))
            t = Tile(reshaped, cell_type)
        except ValueError as e:
            raise ValueError({
                "cell_type": cell_type,
                "cols": cols,
                "rows": rows,
                "cell_data.length": len(cell_data_bytes),
                "cell_data.type": type(cell_data_bytes),
                "cell_data.values": repr(cell_data_bytes)
            }, e)
        return t

    deserialize.__safe_for_unpickling__ = True


Tile.__UDT__ = TileUDT()


class TileExploder(JavaTransformer, JavaMLReadable, JavaMLWritable):
    """
    Python wrapper for TileExploder.scala
    """

    def __init__(self):
        super(TileExploder, self).__init__()
        self._java_obj = self._new_java_obj("org.locationtech.rasterframes.ml.TileExploder", self.uid)


class NoDataFilter(JavaTransformer, HasInputCols, JavaMLReadable, JavaMLWritable):
    """
    Python wrapper for NoDataFilter.scala
    """

    def __init__(self):
        super(NoDataFilter, self).__init__()
        self._java_obj = self._new_java_obj("org.locationtech.rasterframes.ml.NoDataFilter", self.uid)

