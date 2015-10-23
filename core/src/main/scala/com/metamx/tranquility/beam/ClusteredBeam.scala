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
package com.metamx.tranquility.beam

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.event._
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.option._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.typeclass.Timestamper
import com.metamx.tranquility.zk.CachableCoordinatedState
import com.twitter.util.Future
import java.util.UUID
import java.util.concurrent.Executors
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.chrono.ISOChronology
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.Random

/**
 * Beam composed of a stack of smaller beams. The smaller beams are split across two axes: timestamp (time shard
 * of the data) and partition (shard of the data within one time interval). The stack of beams for a particular
 * timestamp are created in a coordinated fashion, such that all ClusteredBeams for the same identifier will have
 * semantically identical stacks. This interaction is mediated through zookeeper. Beam information persists across
 * ClusteredBeam restarts.
 *
 * In the case of Druid, each merged beam corresponds to one segment partition number, and each inner beam corresponds
 * to either one index task or a set of redundant index tasks.
 *
 * {{{
 *                                            ClusteredBeam
 *
 *                                   +-------------+---------------+
 *               2010-01-02T03:00:00 |                             |   2010-01-02T04:00:00
 *                                   |                             |
 *                                   v                             v
 *
 *                         +----+ Merged +----+                   ...
 *                         |                  |
 *                    partition 1         partition 2
 *                         |                  |
 *                         v                  v
 *
 *                     Decorated           Decorated
 *
 *                   InnerBeamType       InnerBeamType
 * }}}
 */
class ClusteredBeam[EventType: Timestamper, InnerBeamType <: Beam[EventType]](
  zkBasePath: String,
  identifier: String,
  tuning: ClusteredBeamTuning,
  curator: CuratorFramework,
  emitter: ServiceEmitter,
  timekeeper: Timekeeper,
  objectMapper: ObjectMapper,
  beamMaker: BeamMaker[EventType, InnerBeamType],
  beamDecorateFn: (Interval, Int) => Beam[EventType] => Beam[EventType],
  beamMergeFn: Seq[Beam[EventType]] => Beam[EventType],
  alertMap: Dict
) extends Beam[EventType] with Logging
{
  require(tuning.partitions > 0, "tuning.partitions > 0")
  require(tuning.minSegmentsPerBeam > 0, "tuning.minSegmentsPerBeam > 0")
  require(
    tuning.maxSegmentsPerBeam >= tuning.minSegmentsPerBeam,
    "tuning.maxSegmentsPerBeam >= tuning.minSegmentsPerBeam"
  )

  // Thread pool for blocking zk operations
  private[this] val zkExec = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("ClusteredBeam-ZkExec-%s" format UUID.randomUUID)
      .build()
  )

  // We will refuse to create beams earlier than this timestamp. The purpose of this is to prevent recreating beams
  // that we thought were closed.
  @volatile private[this] var localLatestCloseTime = new DateTime(0, ISOChronology.getInstanceUTC)

  private[this] val rand = new Random

  // Merged beams we are currently aware of, interval start millis -> merged beam.
  private[this] val beams = ConcurrentMap[Long, Beam[EventType]]()

  // Lock updates to "localLatestCloseTime" and "beams" to prevent races.
  private[this] val beamWriteMonitor = new AnyRef

  private[this] lazy val data = new CachableCoordinatedState[ClusteredBeamMeta](
    curator,
    ZKPaths.makePath(zkBasePath, identifier),
    zkExec,
    ClusteredBeamMeta.empty,
    _.toBytes(objectMapper),
    ClusteredBeamMeta.fromBytes(objectMapper, _)
  )

  @volatile private[this] var open = true

  val timestamper: EventType => DateTime = {
    val theImplicit = implicitly[Timestamper[EventType]].timestamp _
    t => theImplicit(t).withZone(DateTimeZone.UTC)
  }

  private[this] def beam(timestamp: DateTime, now: DateTime): Future[Beam[EventType]] = {
    val bucket = tuning.segmentBucket(timestamp)
    val creationInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start.millis,
      tuning.segmentBucket(Seq(now + tuning.warmingPeriod, now + tuning.windowPeriod).maxBy(_.millis)).end.millis,
      ISOChronology.getInstanceUTC
    )
    val windowInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start.millis,
      tuning.segmentBucket(now + tuning.windowPeriod).end.millis,
      ISOChronology.getInstanceUTC
    )
    val futureBeamOption = beams.get(timestamp.millis) match {
      case _ if !open => Future.value(None)
      case Some(x) if windowInterval.overlaps(bucket) => Future.value(Some(x))
      case Some(x) => Future.value(None)
      case None if timestamp <= localLatestCloseTime => Future.value(None)
      case None if !creationInterval.overlaps(bucket) => Future.value(None)
      case None =>
        // We may want to create new merged beam(s). Acquire the zk mutex and examine the situation.
        // This could be more efficient, but it's happening infrequently so it's probably not a big deal.
        data.modify {
          prev =>
            val prevBeamDicts = prev.beamDictss.getOrElse(timestamp.millis, Nil)
            if (prevBeamDicts.size >= tuning.partitions) {
              log.info(
                "Merged beam already created for identifier[%s] timestamp[%s], with sufficient partitions (target = %d, actual = %d)",
                identifier,
                timestamp,
                tuning.partitions,
                prevBeamDicts.size
              )
              prev
            } else if (timestamp <= prev.latestCloseTime) {
              log.info(
                "Global latestCloseTime[%s] for identifier[%s] has moved past timestamp[%s], not creating merged beam",
                prev.latestCloseTime,
                identifier,
                timestamp
              )
              prev
            } else {
              assert(prevBeamDicts.size < tuning.partitions)
              assert(timestamp > prev.latestCloseTime)

              // We might want to cover multiple time segments in advance.
              val numSegmentsToCover = tuning.minSegmentsPerBeam +
                rand.nextInt(tuning.maxSegmentsPerBeam - tuning.minSegmentsPerBeam + 1)
              val intervalToCover = new Interval(
                timestamp.millis,
                tuning.segmentGranularity.increment(timestamp, numSegmentsToCover).millis,
                ISOChronology.getInstanceUTC
              )
              val timestampsToCover = tuning.segmentGranularity.getIterable(intervalToCover).asScala.map(_.start)

              // OK, create them where needed.
              val newInnerBeamDictsByPartition = new mutable.HashMap[Int, Dict]
              val newBeamDictss: Map[Long, Seq[Dict]] = (prev.beamDictss filterNot {
                case (millis, beam) =>
                  // Expire old beamDicts
                  tuning.segmentGranularity.increment(new DateTime(millis)) + tuning.windowPeriod < now
              }) ++ (for (ts <- timestampsToCover) yield {
                val tsPrevDicts = prev.beamDictss.getOrElse(ts.millis, Nil)
                log.info(
                  "Creating new merged beam for identifier[%s] timestamp[%s] (target = %d, actual = %d)",
                  identifier,
                  ts,
                  tuning.partitions,
                  tsPrevDicts.size
                )
                val tsNewDicts = tsPrevDicts ++ ((tsPrevDicts.size until tuning.partitions) map {
                  partition =>
                    newInnerBeamDictsByPartition.getOrElseUpdate(
                    partition, {
                      // Create sub-beams and then immediately close them, just so we can get the dict representations.
                      // Close asynchronously, ignore return value.
                      beamMaker.newBeam(intervalToCover, partition).withFinally(_.close()) {
                        beam =>
                          val beamDict = beamMaker.toDict(beam)
                          log.info("Created beam: %s", objectMapper.writeValueAsString(beamDict))
                          beamDict
                      }
                    }
                    )
                })
                (ts.millis, tsNewDicts)
              })
              val newLatestCloseTime = new DateTime(
                (Seq(prev.latestCloseTime.millis) ++ (prev.beamDictss.keySet -- newBeamDictss.keySet)).max,
                ISOChronology.getInstanceUTC
              )
              ClusteredBeamMeta(
                newLatestCloseTime,
                newBeamDictss
              )
            }
        } rescue {
          case e: Throwable =>
            Future.exception(
              new IllegalStateException(
                "Failed to save new beam for identifier[%s] timestamp[%s]" format(identifier, timestamp), e
              )
            )
        } map {
          meta =>
            // Update local stuff with our goodies from zk.
            beamWriteMonitor.synchronized {
              localLatestCloseTime = meta.latestCloseTime
              // Only add the beams we actually wanted at this time. This is because there might be other beams in ZK
              // that we don't want to add just yet, on account of maybe they need their partitions expanded (this only
              // happens when they are the necessary ones).
              if (!beams.contains(timestamp.millis) && meta.beamDictss.contains(timestamp.millis)) {
                val beamDicts = meta.beamDictss(timestamp.millis)
                log.info("Adding beams for identifier[%s] timestamp[%s]: %s", identifier, timestamp, beamDicts)
                // Should have better handling of unparseable zk data. Changing BeamMaker implementations currently
                // just causes exceptions until the old dicts are cleared out.
                beams(timestamp.millis) = beamMergeFn(
                  beamDicts.zipWithIndex map {
                    case (beamDict, partitionNum) =>
                      val decorate = beamDecorateFn(tuning.segmentBucket(timestamp), partitionNum)
                      decorate(beamMaker.fromDict(beamDict))
                  }
                )
              }
              // Remove beams that are gone from ZK metadata. They have expired.
              for ((timestamp, beam) <- beams -- meta.beamDictss.keys) {
                log.info("Removing beams for identifier[%s] timestamp[%s]", identifier, timestamp)
                // Close asynchronously, ignore return value.
                beams(timestamp).close()
                beams.remove(timestamp)
              }
              // Return requested beam. It may not have actually been created, so it's an Option.
              beams.get(timestamp.millis) ifEmpty {
                log.info(
                  "Turns out we decided not to actually make beams for identifier[%s] timestamp[%s]. Returning None.",
                  identifier,
                  timestamp
                )
              }
            }
        }
    }
    futureBeamOption map {
      beamOpt =>
        // If we didn't find a beam, then create a special dummy beam just for this batch. This allows us to apply
        // any merge or decorator logic to dropped events, which is nice if there are side effects (such as metrics
        // emission, logging, or alerting).
        beamOpt.getOrElse(
          beamMergeFn(
            (0 until tuning.partitions) map {
              partition =>
                beamDecorateFn(bucket, partition)(new NoopBeam[EventType])
            }
          )
        )
    }
  }

  def propagate(events: Seq[EventType]) = {
    val now = timekeeper.now.withZone(DateTimeZone.UTC)
    val grouped = events.groupBy(x => tuning.segmentBucket(timestamper(x)).start).toSeq.sortBy(_._1.millis)
    // Possibly warm up future beams
    def toBeWarmed(dt: DateTime, end: DateTime): List[DateTime] = {
      if (dt <= end) {
        dt :: toBeWarmed(tuning.segmentBucket(dt).end, end)
      } else {
        Nil
      }
    }
    val warmingBeams = Future.collect(
      for (
        latestEvent <- grouped.lastOption.map(_._2.maxBy(timestamper(_).millis)).map(timestamper).toList;
        tbwTimestamp <- toBeWarmed(latestEvent, latestEvent + tuning.warmingPeriod) if tbwTimestamp > latestEvent
      ) yield {
        // Create beam asynchronously
        beam(tbwTimestamp, now)
      }
    )
    // Propagate data
    val countFutures = for ((timestamp, eventGroup) <- grouped) yield {
      beam(timestamp, now) onFailure {
        e =>
          emitAlert(e, log, emitter, WARN, "Failed to create merged beam: %s" format identifier, alertMap)
      } flatMap {
        beam =>
          // We expect beams to handle retries, so if we get an exception here let's drop the batch
          beam.propagate(eventGroup) rescue {
            case e: DefunctBeamException =>
              // Just drop data until the next segment starts. At some point we should look at doing something
              // more intelligent.
              emitAlert(
                e, log, emitter, WARN, "Beam defunct: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString(),
                    "beam" -> beam.toString
                  )
              )
              data.modify {
                prev =>
                  ClusteredBeamMeta(
                    Seq(prev.latestCloseTime, timestamp).maxBy(_.millis),
                    prev.beamDictss - timestamp.millis
                  )
              } onSuccess {
                meta =>
                  beamWriteMonitor.synchronized {
                    beams.remove(timestamp.millis)
                  }
              } map (_ => 0)

            case e: Exception =>
              emitAlert(
                e, log, emitter, WARN, "Failed to propagate events: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString(),
                    "beams" -> beam.toString
                  )
              )
              Future.value(0)
          }
      }
    }
    val countFuture = Future.collect(countFutures).map(_.sum)
    warmingBeams.flatMap(_ => countFuture) // Resolve only when future beams are warmed up.
  }

  def close() = {
    beamWriteMonitor.synchronized {
      open = false
      val closeFuture = Future.collect(beams.values.toList map (_.close())) map (_ => ())
      beams.clear()
      zkExec.shutdown()
      closeFuture
    }
  }

  override def toString = "ClusteredBeam(%s)" format identifier
}

/**
 * Metadata stored in ZooKeeper for a ClusteredBeam.
 *
 * @param latestCloseTime Most recently shut-down interval (to prevent necromancy).
 * @param beamDictss Map of interval start -> beam metadata, partition by partition.
 */
case class ClusteredBeamMeta(latestCloseTime: DateTime, beamDictss: Map[Long, Seq[Dict]])
{
  def toBytes(objectMapper: ObjectMapper) = objectMapper.writeValueAsBytes(
    Dict(
      // latestTime is only being written for backwards compatibility
      "latestTime" -> new DateTime(
        (Seq(latestCloseTime.millis) ++ beamDictss.map(_._1)).max,
        ISOChronology.getInstanceUTC
      ).toString(),
      "latestCloseTime" -> latestCloseTime.toString(),
      "beams" -> beamDictss.map(kv => (new DateTime(kv._1, ISOChronology.getInstanceUTC).toString(), kv._2))
    )
  )
}

object ClusteredBeamMeta
{
  def empty = ClusteredBeamMeta(new DateTime(0, ISOChronology.getInstanceUTC), Map.empty)

  def fromBytes[A](objectMapper: ObjectMapper, bytes: Array[Byte]): ClusteredBeamMeta = {
    val d = objectMapper.readValue(bytes, classOf[Dict])
    val beams: Map[Long, Seq[Dict]] = dict(d.getOrElse("beams", Dict())) map {
      case (k, vs) =>
        val ts = new DateTime(k, ISOChronology.getInstanceUTC)
        val beamDicts = list(vs) map (dict(_))
        (ts.millis, beamDicts)
    }
    val latestCloseTime = new DateTime(d.getOrElse("latestCloseTime", 0L), ISOChronology.getInstanceUTC)
    ClusteredBeamMeta(latestCloseTime, beams)
  }
}
