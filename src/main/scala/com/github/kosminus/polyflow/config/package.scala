package com.github.kosminus.polyflow

import com.github.kosminus.polyflow.Launcher.logger
import com.github.kosminus.polyflow.app.IApp
import com.github.kosminus.polyflow.context.{BatchContext, EnvContext, StreamingContext}
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader
import net.ceedubs.ficus.readers.ValueReader
import shapeless.{:+:, CNil}

import scala.util.{Failure, Success, Try}

package object config {
  type JobConfiguration = Either[GenericConfiguration,KafkaConfiguration]
  type CoContext = (BatchContext, IApp, Array[String]) :+: (StreamingContext, IApp, Array[String]) :+: CNil

  implicit def jobConfigurationReader : ValueReader[JobConfiguration] =
    (config: Config, path: String) => Try(arbitraryTypeValueReader[GenericConfiguration].value.read(config, path)) match {

    case Success(genericConf) => Left(genericConf)

    case Failure(_) =>
      logger.info("Parsing kafka configuration input")
      Try(arbitraryTypeValueReader[KafkaConfiguration].value.read(config, path)) match {
        case Success(kafkaConf) =>
          logger.info("kafka configuration input is valid")
          Right(kafkaConf)

        case Failure(_) => throw MissingConfigurationException("Not a good job conf. Please check generic or kafka conf")
      }

  }

  implicit def contextConfigurationReader: ValueReader[EnvContext] = (config: Config, path: String) => {
    PolyConfig.job match {
      case Right(_) => StreamingContext()
      case Left(_) =>
        config.as[Option[String]](path) match {
          case Some(context) =>
            context match {
              case "Batch" => BatchContext()
              case "Streaming" => StreamingContext()
              case _ => throw MissingConfigurationException("Please check Context in application.conf file. It should be Batch or Streaming")
            }
          case None => BatchContext()

        }
    }
  }
}
