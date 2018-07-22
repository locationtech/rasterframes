/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource.awspds

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params.HttpMethodParams
import java.io._


/**
 * Common support for downloading data.
 *
 * @since 5/5/18
 */
trait DownloadSupport { self: LazyLogging ⇒
  /**
   * This is probably in the "insanely inefficient" category. Currently just a proof of concept.
   */
  protected val downloadBytes = (s: String) ⇒ {
    val client = new HttpClient()
    val method = new GetMethod(s)
    method.getParams.setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, true))
    method.getParams.setIntParameter(HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, 1024 * 1024 * 100)
    logger.info("Downloading: " + s)
    val status = client.executeMethod(method)
    status match {
      case HttpStatus.SC_OK ⇒ method.getResponseBody
      case _ ⇒ throw new FileNotFoundException(s"Unable to download '$s': ${method.getStatusLine}")
    }
  }
}
