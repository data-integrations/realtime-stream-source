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

import java.util

import co.cask.cdap.api.data.format.{FormatSpecification, StructuredRecord}
import co.cask.cdap.api.data.schema.Schema
import co.cask.cdap.api.spark.JavaSparkExecutionContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.JavaConversions._

/**
  * Scala class to create Dstream
  */
class StructuredRecordStreamInputDStream(sec: JavaSparkExecutionContext,
                                         ssc: StreamingContext,
                                         namespace: String,
                                         streamName: String,
                                         formatSpec: FormatSpecs)
  extends InputDStream[StructuredRecord](ssc: StreamingContext) {

  override def start(): Unit = {
    // no-op
  }

  override def stop(): Unit = {
    // no-op
  }

  override def compute(validTime: Time): Option[RDD[StructuredRecord]] = {
    val endTime = validTime.minus(Seconds(1))
    val startTime = endTime.minus(slideDuration)
    val spec = new FormatSpecification(formatSpec.getName, formatSpec.getSchema, formatSpec.getSettings)

    Some(sec.fromStream(namespace, streamName, spec,
      startTime.milliseconds, endTime.milliseconds, classOf[StructuredRecord]).rdd
      .map(t => {
        val record = t._2.getBody
        val fields = new util.ArrayList[Schema.Field]()
        fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)))
        fields.add(Schema.Field.of("headers",
          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))))
        fields.addAll(record.getSchema.getFields)

        val outputSchema = Schema.recordOf(record.getSchema.getRecordName, fields)
        val builder: StructuredRecord.Builder = StructuredRecord.builder(outputSchema)
        builder.set("ts", t._1)
        builder.set("headers", t._2.getHeaders)
        for (field <- record.getSchema.getFields) {
          val fieldName: String = field.getName
          builder.set(fieldName, record.get(fieldName))
        }
        builder.build()
      }))
  }
}
