package com.github.kosminus.polyflow.providers

import com.github.kosminus.polyflow.config.{Configuration, PolyConfig}
import com.github.kosminus.polyflow.constant.Constants.SparkMaster.LOCAL_MODE
import com.github.kosminus.polyflow.utils.Utils.isLocal
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  val sparkSession: SparkSession = getSparkSession(PolyConfig)

  final def getSparkSession(config:Configuration):SparkSession = {
    val sessionBuilder = SparkSession.builder().config(getSparkConfig(config))

    if (isLocal) sessionBuilder.master(LOCAL_MODE)
    else sessionBuilder.enableHiveSupport()

    val sparkSession = sessionBuilder.getOrCreate()
    sparkSession.sparkContext.setCheckpointDir(config.sparkConfiguration.checkpointDirectory)
    sparkSession
  }

  def getSparkConfig(config:Configuration): SparkConf =
    new SparkConf()
      .setAppName(config.sparkConfiguration.appName)
      .setAll(config.sparkConfiguration.confSettings)


    implicit private class CommonSparkConf(val sparkConf: SparkConf) {
      def setAll(confSettings: Option[Iterable[(String, String)]]): SparkConf =
        confSettings match {
          case Some(values) => sparkConf.setAll(values)
          case None => sparkConf
        }
    }


}