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

package com.metamx.tranquility.druid.tokyodrift

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.druid.TaskClient
import com.metamx.tranquility.druid.TaskPointer
import com.metamx.tranquility.druid.tokyodrift.SlotBeam.AppenderationResponse
import com.metamx.tranquility.finagle.HttpPost
import com.metamx.tranquility.zk.CachableCoordinatedState
import com.twitter.util.Await
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffers

/**
 * A Beam that appenderates events to a fixed set of Druid tasks. The tasks all share a slotNum, and at any given
 * point one of them is leading and the rest are following.
 */
class SlotBeam[A](
  dataSource: String,
  slotNum: Int,
  replicationFactor: Int,
  taskMaker: (String, Int) => Future[TaskPointer],
  clientMaker: (String, TaskPointer) => TaskClient,
  state: CachableCoordinatedState[Meta],
  outputObjectMapper: ObjectMapper,
  outputObjectMapperContentType: String
) extends Beam[A] with Logging with Closable
{
  // Must hold this lock when updating "clients" or "highestSeenEpoch"
  private val updateLock = new AnyRef

  private val clients: scala.collection.concurrent.Map[TaskPointer, TaskClient] = ConcurrentMap()

  @volatile private var highestSeenEpoch: Long = 0L

  private def updateEpoch(newEpoch: Long): Boolean = {
    updateLock.synchronized {
      if (newEpoch > highestSeenEpoch) {
        log.debug(s"Updating epoch for slotNum[$slotNum] from[$highestSeenEpoch] to[$newEpoch].")
        highestSeenEpoch = newEpoch
        true
      } else {
        false
      }
    }
  }

  private def getSlotMeta(): Future[SlotMeta] = {
    val maybeCached: Option[SlotMeta] = state.cachedValue.slots.get(slotNum)
    val activeTasks: Set[TaskPointer] = clients.filter(_._2.active).keySet.toSet

    val cacheValid = maybeCached match {
      case Some(cachedMeta) =>
        cachedMeta.epoch == highestSeenEpoch && activeTasks == cachedMeta.tasks.toSet
      case None => false
    }

    if (cacheValid) {
      Future(maybeCached.get)
    } else {
      log.info(s"Resynchronizing slotMeta for slotNum[$slotNum].")
      state modify { oldMeta =>
        val oldSlotMetaOption: Option[SlotMeta] = oldMeta.slots.get(slotNum)
        val oldSlotMetaValidTasks: Seq[TaskPointer] = oldSlotMetaOption match {
          case Some(oldSlotMeta) => oldSlotMeta.tasks.filterNot(task => clients.get(task).exists(!_.active))
          case None => Nil
        }

        val newTasksNeeded = math.max(0, replicationFactor - oldSlotMetaValidTasks.size)
        val newTasks: Seq[TaskPointer] = oldSlotMetaValidTasks ++ (if (newTasksNeeded > 0) {
          // Need new tasks. Create them while we have the state mutex.
          log.info(
            s"Creating $newTasksNeeded new task(s) for slotNum[$slotNum] with replicationFactor[$replicationFactor]."
          )
          Await.result(
            Future.collect(
              for (i <- 0 until newTasksNeeded) yield {
                taskMaker(dataSource, slotNum)
              }
            )
          )
        } else {
          Nil
        })

        val oldTasks: Seq[TaskPointer] = oldSlotMetaOption.map(_.tasks).getOrElse(Nil)
        val taskListChanged = oldTasks != newTasks
        val newEpoch = (oldSlotMetaOption match {
          case Some(oldSlotMeta) => Seq(highestSeenEpoch, oldSlotMeta.epoch).max
          case None => highestSeenEpoch
        }) + (if (taskListChanged) 1 else 0)

        val newSlotMeta = SlotMeta(slotNum, newEpoch, newTasks)
        if (newSlotMeta != oldSlotMetaOption.orNull) {
          log.info(s"slotMeta for slotNum[$slotNum] changed from[${oldSlotMetaOption.orNull}] to[$newSlotMeta].")
        }
        oldMeta + newSlotMeta
      } map { meta =>
        val slotMeta = meta.slots.apply(slotNum)

        // Adjust our local state based on the new slotMeta.
        updateLock.synchronized {
          updateEpoch(slotMeta.epoch)

          // Close unwanted clients.
          for ((task, client) <- clients.toList if !slotMeta.tasks.contains(task)) {
            // Close asynchronously, ignore return value.
            log.info(s"Closing client for slotNum[$slotNum], task[${task.id}].")
            client.close()
            clients.remove(task)
          }

          // Open new clients.
          for (task <- slotMeta.tasks.toSet -- clients.keySet) {
            log.info(s"Creating new client for slotNum[$slotNum], task[${task.id}].")
            clients.put(task, clientMaker(dataSource, task))
          }
        }

        slotMeta
      } map { slotMeta =>
        slotMeta
      }
    }
  }

  override def propagate(events: Seq[A]): Future[Int] = {
    sendToLeader(events) flatMap {
      case None => Future(0)
      case Some((segments, slotMeta)) =>
        val followers = slotMeta.followers
        val followerClients: Seq[TaskClient] = followers.map(clients.apply)
        val followerFutures: Seq[Future[Unit]] = for (followerClient <- followerClients) yield {
          sendToFollower(followerClient, slotMeta.epoch, events, segments)
        }

        // When done talking to followers, claim that we sent all events that had non-null segments from the leader.
        Future.collect(followerFutures) map (_ => segments.count(_ != null))
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    val tasks = clients.keys.toList
    log.info(s"Closing Druid beam for dataSource[$dataSource], slotNum[$slotNum].")
    val closableClients = tasks.flatMap(clients.remove(_))
    Future.collect(closableClients.map(_.close(deadline))) map (_ => ())
  }

  override def toString = {
    s"SlotBeam(dataSource = $dataSource, slotNum = $slotNum, replicationFactor = $replicationFactor)"
  }

  // Send events to leader, get segments and the slotMeta we used back. Or, if we gave up, a None.
  private def sendToLeader(events: Seq[A]): Future[Option[(Seq[Dict], SlotMeta)]] = {
    log.trace(s"sendToLeader(${events.size} events)")
    getSlotMeta() onSuccess { slotMeta =>
      if (log.isTraceEnabled) {
        log.trace(s"sendToLeader: slotMeta = $slotMeta")
      }
    } flatMap {
      case slotMeta if slotMeta.leader.isEmpty => Future(None)
      case slotMeta =>
        val leader = slotMeta.leader.get
        val leaderClient: TaskClient = clients(leader)
        val requestDict: Dict = Map(
          "leader" -> true,
          "epoch" -> slotMeta.epoch,
          "rows" -> Seq(
            Map(
              "identifier" -> null,
              "rows" -> events
            )
          )
        )
        val requestBytes = outputObjectMapper.writeValueAsBytes(requestDict)
        val request = HttpPost(
          "/druid/worker/v1/chat/%s/appenderate" format leaderClient.task.serviceKey
        ) withEffect {
          req =>
            req.headers.set("Content-Type", outputObjectMapperContentType)
            req.headers.set("Content-Length", requestBytes.length)
            req.setContent(ChannelBuffers.wrappedBuffer(requestBytes))
        }

        if (log.isTraceEnabled) {
          log.trace(
            "Sending %,d events to leader; task[%s], serviceKey[%s]: %s",
            events.size,
            leaderClient.task.id,
            leaderClient.task.serviceKey,
            Jackson.generate(requestDict)
          )
        }

        leaderClient(request) flatMap {
          case Some(httpResponse) =>
            val responseString = httpResponse.getContent.toString(Charsets.UTF_8)
            val responseDict = Jackson.parse[Dict](responseString)
            val response = AppenderationResponse.fromMap(responseDict)

            updateEpoch(response.epoch)
            if (!response.success) {
              // Epoch too old, response failed. Try again!
              log.debug(
                "Leader task[%s] rejected request; response epoch[%,d], request epoch[%,d].",
                leader.id,
                response.epoch,
                slotMeta.epoch
              )

              if (slotMeta.epoch >= response.epoch) {
                // Something's whacky, bail out.
                throw new IllegalStateException(
                  "WTF?! Request epoch[%,d] >= response epoch[%,d] but request still failed?!"
                    format(slotMeta.epoch, response.epoch)
                )
              }

              sendToLeader(events)
            } else {
              Future(Some((response.segments, slotMeta)))
            }

          case None =>
            // Gave up on this leader. It will have been marked inactive, so when we try again we'll pick a new leader.
            assert(!leaderClient.active)
            log.debug("Leader task[%s] went inactive, retrying.", leader.id)
            sendToLeader(events)
        }
    }
  }

  private def sendToFollower(
    client: TaskClient,
    epoch: Long,
    events: Seq[A],
    segments: Seq[Dict]
  ): Future[Unit] =
  {
    log.trace(s"sendToFollower(task = ${client.task.id}, epoch = $epoch)")
    if (events.size != segments.size) {
      throw new IllegalStateException(s"WTF?! events.size[${events.size}] != segments.size[${segments.size}]")
    }

    // Group events by segment, dropping null segments (they represent unparseable or unindexable events)
    val eventsBySegment: Map[Dict, Seq[A]] = (segments zip events).filter(_._1 != null).toMapOfSeqs

    val requestDict: Dict = Map(
      "leader" -> false,
      "epoch" -> epoch,
      "rows" -> (eventsBySegment.toSeq map { case (segment, segmentEvents) =>
        Map(
          "identifier" -> segment,
          "rows" -> segmentEvents
        )
      })
    )
    val requestBytes = outputObjectMapper.writeValueAsBytes(requestDict)
    val request = HttpPost(
      "/druid/worker/v1/chat/%s/appenderate" format client.task.serviceKey
    ) withEffect {
      req =>
        req.headers.set("Content-Type", outputObjectMapperContentType)
        req.headers.set("Content-Length", requestBytes.length)
        req.setContent(ChannelBuffers.wrappedBuffer(requestBytes))
    }

    if (log.isTraceEnabled) {
      log.trace(
        "Sending %,d events to follower; task[%s], serviceKey[%s]: %s",
        events.size,
        client.task.id,
        client.task.serviceKey,
        Jackson.generate(requestDict)
      )
    }

    // TODO: Should actually care about the response; if epoch increased then we should retry the entire propagation
    client(request) map (_ => ())
  }
}

object SlotBeam
{

  case class AppenderationResponse(success: Boolean, epoch: Long, segments: Seq[Dict])

  object AppenderationResponse
  {
    def fromMap(d: Dict): AppenderationResponse = {
      AppenderationResponse(
        bool(d("success")),
        long(d("epoch")),
        list(d("segments")).map(x => dict(x))
      )
    }
  }

}
