package com.github.kosminus.polyflow.reader

import com.github.kosminus.polyflow.config.Sink.HSink
import org.apache.spark.sql.{Dataset, Encoder}
import scala.reflect.runtime.universe.TypeTag
object StreamDatasetContextRead {

  implicit class StreamExtensionreader[T : TypeTag : Encoder](val dataset:Dataset[T]) extends Serializable {
    def hStreamSinkRead(sinkConfigurationInput: HSink): Dataset[T] = {
     val ds = polyRead.apply(sinkConfigurationInput)
    ds
  }}

}
