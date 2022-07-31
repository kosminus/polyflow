package com.github.kosminus.polyflow.reader

import com.github.kosminus.polyflow.Launcher.logger
import com.github.kosminus.polyflow.config.PolyConfig
import com.github.kosminus.polyflow.config.Sink.HSink
import com.github.kosminus.polyflow.context.{BatchContext, StreamingContext}
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.runtime.universe.TypeTag


object DatasetContextRead {
  implicit class Extension[T : TypeTag : Encoder](val dataset:Dataset[T]) extends Serializable {

    def contextRead(sinkConfiguration: HSink): Dataset[T] = {
      val context = PolyConfig.context
      context match {
        case env: StreamingContext =>
          import com.github.kosminus.polyflow.reader.StreamDatasetContextRead._

          logger.info(s"context read : $context")
          dataset.hStreamSinkRead(sinkConfiguration)

        case env : BatchContext =>
          dataset
        case _ => logger.info(s"error reading $context")
            throw MissingConfigurationException("error reading context")

      }
    }
  }

}
