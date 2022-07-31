package com.github.kosminus.polyflow.reader

import com.github.kosminus.polyflow.constant.Constants.{EVENT_BODY_KEY, EVENT_VALUE_KEY}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamReaderExtension {

  implicit class StreamReaderUtils(val sparkSession: SparkSession) extends ReaderUtils {

    def readDataStream(format: String,
                       options: Option[Map[String, String]],
                       path: Option[String]): DataFrame = {

      sparkSession
        .readStream
        .format(format)
        .options(options)
        .load(path)
    }


    def readDataStream(topic: String,
                       castValue: Option[String],
                       kafkaBootstrapServers: String
                       //optionalKafkaOptions: Map[String,String]
                      ): DataFrame = {
      val df = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServers)
        .option("subscribe", topic)
        //.options(optionalKafkaOptions)
        .load()

      val dfCast =
        castValue.flatMap(castV =>
          if (!castV.equals("")) Some(castV.toUpperCase) else None) match {
          case Some(castV) =>
              df.withColumn(EVENT_VALUE_KEY, col(EVENT_VALUE_KEY).cast(castV))
          case None =>
            df
        }

      dfCast.withColumnRenamed(EVENT_VALUE_KEY, EVENT_BODY_KEY)
        .select(EVENT_BODY_KEY)

    }
  }

  implicit class StreamDataReaderUtils(val dataStreamReader: DataStreamReader) {
    def options(options: Option[Map[String, String]]): DataStreamReader =
      options match {
        case Some(o) => dataStreamReader.options(o)
        case None => dataStreamReader
      }

    def load(path: Option[String]): DataFrame =
      path match {
        case Some(p) => dataStreamReader.option("path", p).load()
        case None => dataStreamReader.load()
      }
  }

}
