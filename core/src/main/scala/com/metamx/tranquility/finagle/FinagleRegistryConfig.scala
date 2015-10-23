/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.metamx.tranquility.finagle

import org.scala_tools.time.Imports._

case class FinagleRegistryConfig(
  finagleHttpTimeout: Period = 90.seconds,
  finagleHttpConnectionsPerHost: Int = 2,
  finagleEnableFailFast: Boolean = true
)

object FinagleRegistryConfig
{
  def default() = builder().build()

  /**
   * Builder for FinagleRegistryConfig objects.
   */
  def builder() = new Builder(FinagleRegistryConfig())

  class Builder private[tranquility](config: FinagleRegistryConfig)
  {
    /**
     * Time out HTTP requests that take longer than this.
     */
    def finagleHttpTimeout(x: Period) = new Builder(config.copy(finagleHttpTimeout = x))

    /**
     * Maximum number of HTTP connections we'll make to a particular service.
     */
    def finagleHttpConnectionsPerHost(x: Int) = new Builder(config.copy(finagleHttpConnectionsPerHost = x))

    /**
     * Whether Finagle's fail-fast behavior should be enabled.
     */
    def finagleEnableFailFast(x: Boolean) = new Builder(config.copy(finagleEnableFailFast = x))

    def build(): FinagleRegistryConfig = config
  }

}
