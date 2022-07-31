package com.github.kosminus.polyflow.engine

import com.github.kosminus.polyflow.config.TransferConfiguration
import com.github.kosminus.polyflow.providers.{LoggerProvider, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SparkSession}


trait TransferEngineComponent extends Engine {
  val transferEngine: TransferEngine

  trait TransferEngine extends Engine {
    val sparkSession:SparkSession = SparkSessionProvider.sparkSession

    val config:TransferConfiguration

    val sqlText: Option[String]

    def transfer(input:DataFrame):DataFrame = {
      import sparkSession.implicits._

      val sqlTextDf: DataFrame = sqlText match {
        case Some(someSqlText) =>
          input.createOrReplaceTempView(
            sparkSession.sparkContext.getConf
              .getOption("sql.temporaryTableName")
              .getOrElse("TEMPORARY"))

          val sqlText:String = config.sqlText.getOrElse("select * from TEMPORARY")
          LoggerProvider.logger.info(s"applying sql text to input DF $sqlText")
          sparkSession.sql(sqlText)

        case None =>
          LoggerProvider.logger.info(s"returning input DF")
          input
      }

      sqlTextDf

    }
  }

}
