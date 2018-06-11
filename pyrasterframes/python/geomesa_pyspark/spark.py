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
from pyrasterframes.context import _checked_context


__all__ = ['GeometryUDT']


class GeometryUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: Internal use only.
    """

    @classmethod
    def sqlType(self):
        return StructField("wkb", BinaryType(), False)

    @classmethod
    def module(cls):
        return 'geomesa_pyspark'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.jts.GeometryUDT'

    def serialize(self, obj):
        if (obj is None): return None
        return Row(obj.toBytes)

    def deserialize(self, datum):
        return _checked_context().generateGeometry(datum[0])