/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark

import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.spark.JavaSparkExecutionContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Scala class to create Dstream
  *
  * @param sec
  * @param ssc
  * @param namespace
  * @param streamName
  */
class StreamInputDStream(sec: JavaSparkExecutionContext,
                         ssc: StreamingContext,
                         namespace: String,
                         streamName: String) extends InputDStream[StreamEvent](ssc: StreamingContext) {

  override def start(): Unit = {
    // no-op
  }

  override def stop(): Unit = {
    // no-op
  }

  override def compute(validTime: Time): Option[RDD[StreamEvent]] = {
    val endTime = validTime.minus(Seconds(1))
    val startTime = endTime.minus(slideDuration)

    Some(sec.fromStream(namespace, streamName, startTime.milliseconds, endTime.milliseconds).rdd)
  }
}
