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

import com.metamx.common.Backoff
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import com.twitter.util.Timer
import org.scala_tools.time.Implicits._

object FutureRetry extends Logging
{
  /**
   * Returns a future representing possibly repeated retries of an underlying future creator.
   *
   * @param mkfuture call-by-name future
   * @param isTransients retry if any of these predicates return true for a thrown exception
   * @param timer use this timer for scheduling retries
   */
  def onErrors[A](
    isTransients: Seq[Exception => Boolean],
    backoff: Backoff = Backoff.standard()
  )(mkfuture: => Future[A])(implicit timer: Timer): Future[A] =
  {
    mkfuture rescue {
      case e: Exception if isTransients.exists(_(e)) =>
        new Promise[A] withEffect {
          promise =>
            val next = backoff.next
            backoff.incr()
            log.warn(e, "Transient error, will try again in %s ms", next)
            timer.schedule(Time.now + next.toDuration) {
              promise.become(onErrors(isTransients, backoff)(mkfuture))
            }
        }
    }
  }
}
