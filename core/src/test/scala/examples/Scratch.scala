/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package examples

import astraea.spark.rasterframes._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._

/**
 * Boilerplate test run file
 *
 * @since 10/8/17
 */
object Scratch extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryoserializer.buffer.max", "500m")
    .config("spark.kryo.registrationRequired", "false")
    .config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    .getOrCreate()
    .withRasterFrames

  import spark.implicits._

  // Your Spark code here.....

}
