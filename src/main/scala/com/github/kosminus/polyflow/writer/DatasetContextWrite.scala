package com.github.kosminus.polyflow.writer

import com.github.kosminus.polyflow.Launcher.logger
import com.github.kosminus.polyflow.config.PolyConfig
import com.github.kosminus.polyflow.config.Sink.HSink
import com.github.kosminus.polyflow.context.EnvContext
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import com.github.kosminus.polyflow.providers.SparkSessionProvider
import com.github.kosminus.polyflow.writer.DatasetContextMixWrite.ExtensionWriter
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}


object DatasetContextWrite {
  implicit class Extension[T: TypeTag](val dataset: Dataset[T]) extends Serializable {
    def contextWrite(sinkConfiguration: HSink): Unit = {
      val sparkSession = SparkSessionProvider.sparkSession

      val context = PolyConfig.context match {
        case _: EnvContext =>
          Try(dataset.hSinkWrite(sinkConfiguration)) match {
            case Success(value) => logger.info("dataset to be written")
            case Failure(exception) => exception.printStackTrace()
          }

        case _ => throw  MissingConfigurationException(s"no good context ")
      }
    }
  }
}


