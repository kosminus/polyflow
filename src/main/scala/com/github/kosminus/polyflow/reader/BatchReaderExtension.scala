package com.github.kosminus.polyflow.reader

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object BatchReaderExtension {

  implicit class BatchReaderUtils(val sparkSession: SparkSession) extends ReaderUtils {

    def readDataFrame(format: String,
                      options: Option[Map[String, String]],
                      path: Option[String]): DataFrame = {

      readFiles(format, options, path)
    }


    def readFiles(format: String,
                  options: Option[Map[String, String]],
                  path: Option[String]): DataFrame = {
      val df = sparkSession
        .read
        .format(format)
        .options(options)
        .load(path)
      df
    }
  }

  implicit class DataFrameReaderUtils(val dataFrameReader: DataFrameReader) {
    def options(options: Option[Map[String, String]]): DataFrameReader =
      options match {
        case Some(o) => dataFrameReader.options(o)
        case None => dataFrameReader
      }

    def load(path: Option[String]): DataFrame =
      path match {
        case Some(p) => dataFrameReader.load(p)
        case None => dataFrameReader.load()
      }
  }

}
