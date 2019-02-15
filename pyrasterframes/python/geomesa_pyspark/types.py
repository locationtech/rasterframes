"""***********************************************************************
   This file was created by Astraea, Inc., 2018 from an excerpt of the
   original:

   Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
   All rights reserved. This program and the accompanying materials
   are made available under the terms of the Apache License, Version 2.0
   which accompanies this distribution and is available at
   http://www.opensource.org/licenses/apache2.0.php.
+ ***********************************************************************/"""

from pyspark.sql.types import UserDefinedType
from pyspark.sql import Row
from pyspark.sql.types import *
from pyrasterframes.context import RFContext

class GeometryUDT(UserDefinedType):
    @classmethod
    def sqlType(self):
        #    return StructField("wkb", BinaryType(), False)
        return StructType([StructField("wkb", BinaryType(), True)])

    @classmethod
    def module(cls):
        return 'geomesa_pyspark.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.jts.' + cls.__name__

    def serialize(self, obj):
        if (obj is None): return None
        return Row(obj.toBytes)

    def deserialize(self, datum):
        return RFContext._jvm_mirror().generate_geometry(datum[0])


class PointUDT(GeometryUDT):
    pass


class LineStringUDT(GeometryUDT):
    pass


class PolygonUDT(GeometryUDT):
    pass


class MultiPointUDT(GeometryUDT):
    pass


class MultiLineStringUDT(GeometryUDT):
    pass


class MultiPolygonUDT(GeometryUDT):
    pass


class GeometryUDT(GeometryUDT):
    pass


class GeometryCollectionUDT(GeometryUDT):
    pass
