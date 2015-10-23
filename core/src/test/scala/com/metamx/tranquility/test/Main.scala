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

package com.metamx.tranquility.test

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.Granularity
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.logger.Logger
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.net.curator.Curator
import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.net.curator.DiscoConfig
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.core.LoggingEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.druid.DruidEnvironment
import com.metamx.tranquility.druid.DruidGuicer
import com.metamx.tranquility.druid.IndexService
import com.metamx.tranquility.druid.IndexServiceConfig
import com.metamx.tranquility.druid.SpecificDruidDimensions
import com.metamx.tranquility.druid.TaskClient
import com.metamx.tranquility.druid.TaskPointer
import com.metamx.tranquility.druid.tokyodrift.Meta
import com.metamx.tranquility.druid.tokyodrift.SlotBeam
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.metamx.tranquility.zk.CachableCoordinatedState
import com.twitter.util.Await
import com.twitter.util.Future
import io.druid.data.input.impl.JSONParseSpec
import io.druid.data.input.impl.MapInputRowParser
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.AggregatorFactory
import io.druid.query.aggregation.CountAggregatorFactory
import io.druid.segment.indexing.DataSchema
import io.druid.segment.indexing.granularity.UniformGranularitySpec
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import javax.ws.rs.core.MediaType
import org.scala_tools.time.Imports._
import scala.collection.JavaConverters._

object Main extends Logging
{
  val zkExec = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder()
      .setNameFormat("zkExec-%d")
      .setDaemon(true)
      .build()
  )

  def main(args: Array[String]): Unit = {
    val slotNum = 0
    val replicationFactor = 3
    val zkStatePath = "/tokyodrift"
    val dataSource = "foo5"
    val lifecycle = new Lifecycle
    val curatorLifecycle = new Lifecycle
    lifecycle onStop {
      curatorLifecycle.stop()
    }
    val curator = Curator.create("localhost:2181", 20.seconds, curatorLifecycle)
    curatorLifecycle.start()
    val objectMapper = Jackson.newObjectMapper()
    val emitter = new ServiceEmitter(
      "service",
      "host",
      new LoggingEmitter(new Logger(getClass), LoggingEmitter.Level.INFO, objectMapper)
    )
    emitter.start()
    val finagleRegistry = new FinagleRegistry(
      FinagleRegistryConfig.default(),
      new Disco(
        curator,
        new DiscoConfig
        {
          override def discoAnnounce = None

          override def discoPath = "/druid/discovery"
        }
      )
    )
    val indexService = new IndexService(
      new DruidEnvironment("overlord", "%s"),
      new IndexServiceConfig
      {
        override def indexRetryPeriod = 1.minute
      },
      finagleRegistry,
      DruidGuicer.objectMapper,
      lifecycle
    )
    lifecycle.start()
    val state = new CachableCoordinatedState[Meta](
      curator,
      zkStatePath,
      zkExec,
      Meta.empty,
      meta => Jackson.bytes(meta.toMap),
      bytes => Meta.fromMap(Jackson.parse[Dict](bytes))
    )
    val taskMaker = makeTaskMaker(indexService) _
    val clientMaker = makeClientMaker(finagleRegistry, indexService, emitter) _
    val slotBeam = new SlotBeam[Dict](
      dataSource,
      slotNum,
      replicationFactor,
      taskMaker,
      clientMaker,
      state,
      objectMapper,
      MediaType.APPLICATION_JSON
    )

    val sent = new AtomicLong
    val done = new AtomicBoolean()
    val finished = new CountDownLatch(1)

    val thread = new Thread {
      override def run(): Unit = {
        try {
          Iterator.continually {
            Dict(
              "timestamp" -> DateTime.now,
              "dim1" -> UUID.randomUUID().toString
            )
          } grouped 250 foreach { messages =>
            sent.addAndGet(Await.result(slotBeam.propagate(messages)).toLong)
            if (done.get()) {
              throw new RuntimeException("DONE")
            }
          }
        } catch {
          case e: Exception =>
            log.warn(e, "Push failed, stopping")
        }

        finished.countDown()
      }
    }

    thread.setDaemon(false)
    thread.start()

    Runtime.getRuntime.addShutdownHook(
      new Thread
      {
        override def run(): Unit = {
          println("interrupting")
          done.set(true)
          finished.await()
          println(s"sent ${sent.get()} messages")
        }
      }
    )

    Thread.currentThread().join()
  }

  def makeTaskMaker(indexService: IndexService)(dataSource: String, slotNum: Int): Future[TaskPointer] = {
    val taskJson = DruidGuicer.objectMapper.writeValueAsBytes(
      Dict(
        "type" -> "index_realtime_tokyo_drift",
        "resource" -> Dict().asJava,
        "slotNum" -> slotNum,
        "dataSchema" -> new DataSchema(
          dataSource,
          new MapInputRowParser(
            new JSONParseSpec(
              new TimestampSpec("timestamp", "auto", null),
              SpecificDruidDimensions(IndexedSeq("dim1", "dim2")).spec
            )
          ),
          Array[AggregatorFactory](
            new CountAggregatorFactory("cnt")
          ),
          new UniformGranularitySpec(
            Granularity.MINUTE,
            QueryGranularity.NONE,
            null
          )
        ),
        "tuningConfig" -> Dict("type" -> "realtime", "windowPeriod" -> "PT10S").asJava
      ).asJava
    )
    indexService.submit(taskJson) map { taskId =>
      TaskPointer(taskId, taskId)
    }
  }

  def makeClientMaker(finagleRegistry: FinagleRegistry, indexService: IndexService, emitter: ServiceEmitter)
      (dataSource: String, task: TaskPointer): TaskClient =
  {
    new TaskClient(
      task,
      finagleRegistry.checkout(task.id),
      dataSource,
      15.seconds,
      1.minute,
      indexService,
      emitter
    )
  }
}
