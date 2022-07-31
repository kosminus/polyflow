package com.github.kosminus.polyflow.config

import com.github.kosminus.polyflow.constant.Constants.SinkType
import com.github.kosminus.polyflow.providers.LoggerProvider
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader
import shapeless._

import scala.util.{Failure, Success, Try}

object Sink {
  type HSink =
    Option[ConsoleSinkConfiguration] ::
      Option[HiveSinkConfiguration] ::
      Option[GenericSinkConfiguration] ::
      Option[KafkaSinkConfiguration] ::
      Option[JDBCSinkConfiguration] ::
      Option[CassandraSinkConfiguration] ::
      Option[BigQuerySinkConfiguration] :: HNil

  def apply(config: Config, path: String): Map[String, HSink] = {
    read(config, path)
  }


  def read(config: Config, path: String): Map[String, HSink] = {

    def readSink[T: ValueReader](config: Config, innerpath: String): Option[T] = {
      Try(config.as[T](innerpath)) match {
        case Success(sinkConfiguration) =>
          LoggerProvider.logger.info(s"Sink read successfully $sinkConfiguration")
          Some(sinkConfiguration)
        case Failure(_) =>
          LoggerProvider.logger.info(s"Sink not provided $path")
          None
      }
    }

    val mapInter = scala.collection.mutable.Map[String, HSink]()
    val configs: Seq[(String, Config)] = config.as[Map[String, Config]](path).toList
    for ((key, conf) <- configs) {
      val consoleSink: Option[ConsoleSinkConfiguration] = readSink[ConsoleSinkConfiguration](conf, SinkType.console)
      val hiveSink: Option[HiveSinkConfiguration] = readSink[HiveSinkConfiguration](conf, SinkType.hive)
      val genericSink: Option[GenericSinkConfiguration] = readSink[GenericSinkConfiguration](conf, SinkType.generic)
      val kafkaSink: Option[KafkaSinkConfiguration] = readSink[KafkaSinkConfiguration](conf, SinkType.kafka)
      val jdbcSink: Option[JDBCSinkConfiguration] = readSink[JDBCSinkConfiguration](conf, SinkType.jdbc)
      val cassandraSink: Option[CassandraSinkConfiguration] = readSink[CassandraSinkConfiguration](conf, SinkType.cassandra)
      val bigQuerySink: Option[BigQuerySinkConfiguration] = readSink[BigQuerySinkConfiguration](conf, SinkType.bigquery)


      if (consoleSink.isEmpty && hiveSink.isEmpty && genericSink.isEmpty && kafkaSink.isEmpty && cassandraSink.isEmpty  && bigQuerySink.isEmpty )
        LoggerProvider.logger.debug(s"Sink is not provided.Please check application.conf")

      mapInter(key) = consoleSink :: hiveSink :: genericSink :: kafkaSink :: jdbcSink :: cassandraSink  :: bigQuerySink :: HNil

    }
    mapInter.map(kv => (kv._1, kv._2)).toMap

  }
}

