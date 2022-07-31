package com.github.kosminus.polyflow.reader

import com.github.kosminus.polyflow.config.KafkaSinkConfiguration
import com.github.kosminus.polyflow.config.Sink.HSink
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import com.github.kosminus.polyflow.providers.{LoggerProvider, SparkSessionProvider}
import com.github.kosminus.polyflow.reader.StreamReaderExtension.StreamReaderUtils
import org.apache.spark.sql.{Dataset, Encoder}
import shapeless.Poly1

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

object polyRead extends Poly1 {
  val logger = LoggerProvider.logger

  implicit def caseStructuredStreamingContext[T: TypeTag : Encoder]: Case.Aux[HSink, Dataset[T]] =
    at((sinkConfigurationAsInput) => {

      val sparkSession = SparkSessionProvider.sparkSession
      import sparkSession.implicits._

      val optionHexKafkaConf: Option[KafkaSinkConfiguration] = Try(sinkConfigurationAsInput.select[Option[KafkaSinkConfiguration]]) match {
        case Success(kafkaSinkConfiguration: Option[KafkaSinkConfiguration]) => kafkaSinkConfiguration
        case Failure(ex) =>
          logger.error(ex.toString)
          logger.debug("no kafka conf")
          throw ex
      }

      val hexKafkaConf = optionHexKafkaConf match {
        case Some(conf) => conf
        case None => throw MissingConfigurationException("")
      }

      val options = Map("kafka.bootstrap.servers" -> hexKafkaConf.bootstrapServers, "subscribe" -> hexKafkaConf.topic, "startingOffsets" -> "earliest")
      val format = "kafka"
      val inputData = sparkSession.readDataStream(format, Some(options), None)


      val ds = inputData
        .selectExpr("CAST(value as STRING")
        .as[String]
        .map { x =>
          val splitString = x.split(",")
          (splitString.head.drop(1), splitString(1).dropRight(1))
        }.as[T]

      ds
    })
}
