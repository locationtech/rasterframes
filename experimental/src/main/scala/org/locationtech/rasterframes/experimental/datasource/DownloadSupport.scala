/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.experimental.datasource

import java.io._
import java.net

import com.typesafe.scalalogging.Logger
import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params.HttpMethodParams
import org.slf4j.LoggerFactory
import spray.json._


/**
 * Common support for downloading data.
 * This is probably in the "insanely inefficient" category. Currently just a proof of concept.
 *
 * @since 5/5/18
 */
trait DownloadSupport {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  private def applyMethodParams[M <: HttpMethodBase](method: M): M = {
    method.getParams.setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, true))
    method.getParams.setIntParameter(HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, 1024 * 1024 * 100)
    method
  }

  private def doGet[T](uri: java.net.URI, handler: HttpMethodBase ⇒ T): T = {
    val client = new HttpClient()
    val method = applyMethodParams(new GetMethod(uri.toASCIIString))
    logger.debug("Requesting " + uri)
    val status = client.executeMethod(method)
    status match {
      case HttpStatus.SC_OK ⇒ handler(method)
      case _ ⇒ throw new FileNotFoundException(s"Unable to download '$uri': ${method.getStatusLine}")
    }
  }

  protected def getBytes(uri: net.URI): Array[Byte] = doGet(uri, _.getResponseBody)
  protected def getJson(uri: net.URI): JsValue = doGet(uri, _.getResponseBodyAsString.parseJson)

}
