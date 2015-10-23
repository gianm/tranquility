/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.zk

import com.metamx.common.scala.Logging
import com.twitter.util.Future
import com.twitter.util.FuturePool
import java.util.concurrent.ExecutorService
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.apache.zookeeper.data.Stat

class CachableCoordinatedState[A](
  curator: CuratorFramework,
  zkBasePath: String,
  executor: ExecutorService,
  defaultValue: A,
  toBytes: A => Array[Byte],
  fromBytes: Array[Byte] => A
) extends Logging
{
  private val futurePool: FuturePool = FuturePool(executor)

  private val mutex = new InterProcessSemaphoreMutex(curator, mutexPath)

  @volatile private var _cachedValue: A = defaultValue

  private def dataPath = ZKPaths.makePath(zkBasePath, "data")

  private def mutexPath = ZKPaths.makePath(zkBasePath, "mutex")

  private def createIfNeeded(): Unit = {
    if (curator.checkExists().forPath(dataPath) == null) {
      try {
        curator.create().creatingParentsIfNeeded().forPath(dataPath, toBytes(defaultValue))
      }
      catch {
        case e: NodeExistsException => // someone beat us to it, suppress
      }
    }
  }

  def cachedValue: A = _cachedValue

  def modify(f: A => A): Future[A] = futurePool {
    createIfNeeded()
    mutex.acquire()
    try {
      curator.sync().forPath(dataPath)
      val stat = new Stat
      val prevBytes = curator.getData.storingStatIn(stat).forPath(dataPath)
      val prevObject = try fromBytes(prevBytes)
      catch {
        case e: Exception =>
          throw new IllegalStateException("Failed to read state at zkBasePath[%s]" format zkBasePath, e)
      }
      val newObject = f(prevObject)
      if (newObject != prevObject) {
        val newBytes = toBytes(newObject)
        log.info("Writing new state to[%s]: %s", dataPath, newObject)
        // Should never have a version mismatch, since we've acquired a mutex.
        // If it does fail, let the exception bubble out.
        curator.setData().withVersion(stat.getVersion).forPath(dataPath, newBytes)
      } else {
        log.info("No need to write new state to[%s], already matched: %s", dataPath, newObject)
      }
      _cachedValue = newObject
      newObject
    }
    catch {
      case e: Throwable =>
        // Log Throwables to avoid invisible errors caused by https://github.com/twitter/util/issues/100.
        log.error(e, "Failed to update state at zkBasePath[%s]", zkBasePath)
        throw e
    }
    finally {
      mutex.release()
    }
  }

}
