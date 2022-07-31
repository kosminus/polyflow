package com.github.kosminus.polyflow.writer

import com.github.kosminus.polyflow.config.{BigQuerySinkConfiguration, CassandraSinkConfiguration, ConsoleSinkConfiguration, GenericSinkConfiguration, HiveSinkConfiguration, JDBCSinkConfiguration, KafkaSinkConfiguration}
import com.github.kosminus.polyflow.constant.Constants
import com.github.kosminus.polyflow.constant.Constants.{KAFKA_JSON_VALUES, KAFKA_KEY_NULL_VALUE}
import com.github.kosminus.polyflow.context.{BatchContext, EnvContext, StreamingContext}
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import org.apache.spark.sql._
import shapeless.Poly3

object polyWrite extends Poly3 {
  implicit def caseHiveSink[T]: Case.Aux[HiveSinkConfiguration, Dataset[T], EnvContext, Unit] =
    at { (hiveSinkConfiguration, dataset, envContext) => {
      envContext match {
        case _: BatchContext =>
          dataset.write
            .format(hiveSinkConfiguration.format)
            .mode(hiveSinkConfiguration.saveMode.getOrElse(SaveMode.Append).toString)
            .options(hiveSinkConfiguration.options.getOrElse(Map.empty))
            .partitionBy(hiveSinkConfiguration.partitionBy.getOrElse(List[String]()): _*)
            .saveAsTable(hiveSinkConfiguration.tableName)
        case _: StreamingContext =>
          dataset.writeStream
            .format(hiveSinkConfiguration.format)
            .options(hiveSinkConfiguration.options.getOrElse(Map.empty))
            .partitionBy(hiveSinkConfiguration.partitionBy.getOrElse(List[String]()): _*)
            .toTable(hiveSinkConfiguration.tableName)
            .awaitTermination()
      }
    }
    }

  implicit def caseConsoleSink[T]: Case.Aux[ConsoleSinkConfiguration, Dataset[T], EnvContext, Unit] =
    at { (consoleSinkConfiguration, dataset, envContext) => {
      envContext match {
        case _: BatchContext =>
          (consoleSinkConfiguration.numRows, consoleSinkConfiguration.truncate) match {
            case (Some(r), Some(t)) => dataset.show(r, t)
            case (Some(r), None) => dataset.show(r)
            case (None, Some(t)) => dataset.show(t)
            case (None, None) => dataset.show()
          }
        case _: StreamingContext =>
          dataset.writeStream
            .format(Constants.SinkType.console.toLowerCase())
            .outputMode(consoleSinkConfiguration.outputMode.getOrElse(Constants.APPEND))
            .start()
            .awaitTermination()
      }
    }
    }


  implicit def caseKafkaSink[T]: Case.Aux[KafkaSinkConfiguration, Dataset[T], EnvContext, Unit] = {
    at { (kafkaSinkConfiguration, dataset, envContext) => {
      envContext match {
        case _: StreamingContext =>
          dataset.selectExpr(kafkaSinkConfiguration.selectExprKey.getOrElse(KAFKA_KEY_NULL_VALUE),
            kafkaSinkConfiguration.selectExprValue.getOrElse(KAFKA_JSON_VALUES))
            .writeStream
            .format(Constants.KAFKA)
            .option("kafka.bootstrap.servers", kafkaSinkConfiguration.bootstrapServers)
            .option("topic", kafkaSinkConfiguration.topic)
            .option("checkpointLocation", kafkaSinkConfiguration.checkpointLocation)
            .start()
            .awaitTermination()
        case envContext: BatchContext =>
          throw  MissingConfigurationException("kafka works only on streaming")
      }
    }

    }
  }

  implicit def caseGenericSink[T]: Case.Aux[GenericSinkConfiguration, Dataset[T], EnvContext, Unit] = {
    at { (genericSinkConfiguration, dataset, envContext) => {
      envContext match {
        case _: BatchContext =>
          dataset.write
            .format(genericSinkConfiguration.format)
            .mode(genericSinkConfiguration.saveMode.getOrElse(Constants.APPEND))
            .options(genericSinkConfiguration.options)
            .partitionBy(genericSinkConfiguration.partitionBy.getOrElse(List[String]()): _*)
            .save(genericSinkConfiguration.path)
        case _: StreamingContext =>
          dataset.writeStream
            .format(genericSinkConfiguration.format)
            .options(genericSinkConfiguration.options)
            .partitionBy(genericSinkConfiguration.partitionBy.getOrElse(List[String]()): _*)
            .start(genericSinkConfiguration.path)
            .awaitTermination()
      }
    }
    }
  }


  implicit def caseBigQuerySinkBatch[T]: Case.Aux[BigQuerySinkConfiguration, Dataset[T], EnvContext, Unit] = {
    at { (bigQuerySinkConfiguration, dataset, envContext) => {
      envContext match {
        case _: BatchContext =>
          dataset.write
            .format(bigQuerySinkConfiguration.format)
            .option("table", s"${bigQuerySinkConfiguration.project}.${bigQuerySinkConfiguration.dataset}.${bigQuerySinkConfiguration.table}")
            .option("temporaryGcsBucket", bigQuerySinkConfiguration.temporaryGcsBucket)
            .mode(bigQuerySinkConfiguration.saveMode.getOrElse(Constants.APPEND))
            .save()
        case _: StreamingContext =>
          dataset.writeStream
            .format(bigQuerySinkConfiguration.format)
            .option("table", s"${bigQuerySinkConfiguration.project}.${bigQuerySinkConfiguration.dataset}.${bigQuerySinkConfiguration.table}")
            .start()
            .awaitTermination()
      }
    }
    }
  }


    implicit def caseJdbcSink[T]: Case.Aux[JDBCSinkConfiguration, Dataset[T], EnvContext, Unit] = {
      at { (jdbcSinkConfiguration, dataset, envContext) => {
        envContext match {
          case _: BatchContext =>
            dataset.write
              .format("jdbc")
              .option("driver", jdbcSinkConfiguration.driver)
              .option("url", jdbcSinkConfiguration.url)
              .option("dbtable", jdbcSinkConfiguration.dbTable)
              .option("user", jdbcSinkConfiguration.user)
              .option("password", jdbcSinkConfiguration.password)
              .mode(jdbcSinkConfiguration.mode.getOrElse(SaveMode.Append).toString)
              .save()

          case _: StreamingContext =>
            throw new MissingConfigurationException("jdbc sink is not ok on streaming")
        }
      }
      }
    }

    implicit def caseCassandraSink[T]: Case.Aux[CassandraSinkConfiguration, Dataset[T], EnvContext, Unit] = {
      at { (cassandraSinkConfiguration, dataset, envContext) => {
        envContext match {
          case _: BatchContext =>
            dataset
              .write
              .format(cassandraSinkConfiguration.format)
              .mode(cassandraSinkConfiguration.saveMode.getOrElse(Constants.APPEND))
              .options(Map("keyspace" -> cassandraSinkConfiguration.keyspace, "table" -> cassandraSinkConfiguration.table))
              .save()

          case _: StreamingContext =>
            dataset
              .writeStream
              .format(cassandraSinkConfiguration.format)
              .options(Map("keyspace" -> cassandraSinkConfiguration.keyspace, "table" -> cassandraSinkConfiguration.table))
              .start()
              .awaitTermination()
        }
      }
      }
    }
  }


