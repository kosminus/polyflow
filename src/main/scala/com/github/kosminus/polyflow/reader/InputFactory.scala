package com.github.kosminus.polyflow.reader

import com.github.kosminus.polyflow.config.{GenericConfiguration, KafkaConfiguration}
import com.github.kosminus.polyflow.context.{BatchContext, EnvContext, StreamingContext}
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import com.github.kosminus.polyflow.model.TopicAndData
import com.github.kosminus.polyflow.providers.SparkSessionProvider
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

object InputFactory {
  @transient val sparkSession = SparkSessionProvider.sparkSession

  def getDataGeneric(configuration: GenericConfiguration, envContext: EnvContext ): Iterable[TopicAndData[DataFrame]] = {
    val readDataFunction = getReadDataFunction(envContext)
    import BatchReaderExtension._
    Try(sparkSession.readData(configuration.format, configuration.options, configuration.paths.get, readDataFunction)) match {
      case Success(data) =>
        Seq(TopicAndData(configuration.topic, data))
      case Failure(ex) =>
        getDataFailure(ex)
    }
  }

  def getReadDataFunction(envContext: EnvContext): (String, Option[Map[String, String]], Option[String]) => DataFrame = {
    import BatchReaderExtension.BatchReaderUtils
    import com.github.kosminus.polyflow.reader.StreamReaderExtension.StreamReaderUtils
    envContext match {
      case StreamingContext() =>
        sparkSession.readDataStream
      case BatchContext() =>
        sparkSession.readDataFrame
    }
  }

  def getDataFailure(ex: Throwable): Nothing = {
    ex match {
      case _ =>
        throw  MissingConfigurationException("input",ex)

    }
  }

  def getDataKafka(kafkaConf: KafkaConfiguration): Iterable[TopicAndData[DataFrame]] = {
    import com.github.kosminus.polyflow.reader.StreamReaderExtension.StreamReaderUtils
    kafkaConf.topics.map { topic =>
      Try(sparkSession.readDataStream(topic, kafkaConf.castValue, kafkaConf.kafkaBootstrapServers)) match {
        case Success(data) =>
          TopicAndData(topic, data)
        case Failure(exception) =>
          throw MissingConfigurationException("get data kafka failed",exception)
      }

    }
  }
}
