import math

import numpy as np
import numpy.testing
import pandas
import pyspark.sql.functions as F
from pyproj import CRS as pyCRS
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StructField, StructType


def test_mask_no_data():
    t1 = Tile(np.array([[1, 2], [3, 4]]), CellType("int8ud3"))
    assert t1.cells.mask[1][0]
    assert t1.cells[1][1] is not None
    assert len(t1.cells.compressed()) == 3

    t2 = Tile(np.array([[1.0, 2.0], [float("nan"), 4.0]]), CellType.float32())
    assert len(t2.cells.compressed()) == 3
    assert t2.cells.mask[1][0]
    assert t2.cells[1][1] is not None


def test_tile_udt_serialization(spark):

    udt = TileUDT()
    cell_types = (
        ct for ct in rf_cell_types() if not (ct.is_raw() or ("bool" in ct.base_cell_type_name()))
    )

    for ct in cell_types:
        cells = (100 + np.random.randn(3, 3) * 100).astype(ct.to_numpy_dtype())

        if ct.is_floating_point():
            nd = 33.0
        else:
            nd = 33

        cells[1][1] = nd
        a_tile = Tile(cells, ct.with_no_data_value(nd))
        round_trip = udt.fromInternal(udt.toInternal(a_tile))
        assert a_tile == round_trip, "round-trip serialization for " + str(ct)

        schema = StructType([StructField("tile", TileUDT(), False)])
        df = spark.createDataFrame([{"tile": a_tile}], schema)

        long_trip = df.first()["tile"]
        assert long_trip == a_tile


def test_masked_deser(spark):
    t = Tile(np.array([[1, 2, 3,], [4, 5, 6], [7, 8, 9]]), CellType("uint8"))

    df = spark.createDataFrame([Row(t=t)])
    roundtrip = df.select(rf_mask_by_value("t", rf_local_greater("t", lit(6)), 1)).first()[0]
    assert roundtrip.cells.mask.sum() == 3, (
        f"Expected {3} nodata values but found Tile" f"{roundtrip}"
    )


def test_udf_on_tile_type_input(spark, img_uri, rf):

    df = spark.read.raster(img_uri)

    # create trivial UDF that does something we already do with raster_Functions
    @F.udf("integer")
    def my_udf(t):
        a = t.cells
        return a.size  # same as rf_dimensions.cols * rf_dimensions.rows

    rf_result = rf.select(
        (rf_dimensions("tile").cols.cast("int") * rf_dimensions("tile").rows.cast("int")).alias(
            "expected"
        ),
        my_udf("tile").alias("result"),
    ).toPandas()

    numpy.testing.assert_array_equal(rf_result.expected.tolist(), rf_result.result.tolist())

    df_result = df.select(
        (
            rf_dimensions(df.proj_raster).cols.cast("int")
            * rf_dimensions(df.proj_raster).rows.cast("int")
            - my_udf(rf_tile(df.proj_raster))
        ).alias("result")
    ).toPandas()

    numpy.testing.assert_array_equal(np.zeros(len(df_result)), df_result.result.tolist())


def test_udf_on_tile_type_output(rf):

    # create a trivial UDF that does something we already do with a raster_functions
    @F.udf(TileUDT())
    def my_udf(t):
        import numpy as np

        return Tile(np.log1p(t.cells))

    rf_result = rf.select(
        rf_tile_max(rf_local_subtract(my_udf(rf.tile), rf_log1p(rf.tile))).alias("expect_zeros")
    ).collect()

    # almost equal because of different implemenations under the hoods: C (numpy) versus Java (rf_)
    numpy.testing.assert_almost_equal(
        [r["expect_zeros"] for r in rf_result], [0.0 for _ in rf_result], decimal=6
    )


def test_no_data_udf_handling(spark):

    t1 = Tile(np.array([[1, 2], [0, 4]]), CellType.uint8())
    assert t1.cell_type.to_numpy_dtype() == np.dtype("uint8")
    e1 = Tile(np.array([[2, 3], [0, 5]]), CellType.uint8())
    schema = StructType([StructField("tile", TileUDT(), False)])
    df = spark.createDataFrame([{"tile": t1}], schema)

    @F.udf(TileUDT())
    def increment(t):
        return t + 1

    r1 = df.select(increment(df.tile).alias("inc")).first()["inc"]
    assert r1 == e1


def test_udf_np_implicit_type_conversion(spark):

    a1 = np.array([[1, 2], [0, 4]])
    t1 = Tile(a1, CellType.uint8())
    exp_array = a1.astype(">f8")

    @F.udf(TileUDT())
    def times_pi(t):
        return t * math.pi

    @F.udf(TileUDT())
    def divide_pi(t):
        return t / math.pi

    @F.udf(TileUDT())
    def plus_pi(t):
        return t + math.pi

    @F.udf(TileUDT())
    def less_pi(t):
        return t - math.pi

    df = spark.createDataFrame(pandas.DataFrame([{"tile": t1}]))
    r1 = df.select(less_pi(divide_pi(times_pi(plus_pi(df.tile))))).first()[0]

    assert np.all(r1.cells == exp_array)
    assert r1.cells.dtype == exp_array.dtype


def test_crs_udt_serialization():
    udt = CrsUDT()

    crs = CRS(pyCRS.from_epsg(4326).to_proj4())

    roundtrip = udt.fromInternal(udt.toInternal(crs))
    assert crs == roundtrip


def test_extract_from_raster(spark, img_uri):
    # should be able to write a projected raster tile column to path like '/data/foo/file.tif'

    rf = spark.read.raster(img_uri)
    crs: DataFrame = rf.select(rf_crs("proj_raster").alias("crs")).distinct()
    assert crs.schema.fields[0].dataType == CrsUDT()
    assert crs.first()["crs"].proj4_str == "+proj=utm +zone=16 +datum=WGS84 +units=m +no_defs "
