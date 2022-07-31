package com.github.kosminus.polyflow.job

import com.github.kosminus.polyflow.constant.Constants.LOG4J_PROPERTIES_PATH
import com.github.kosminus.polyflow.providers.{LoggerProvider, SparkSessionProvider}
import org.apache.log4j.PropertyConfigurator


trait IJob {
  val spark = SparkSessionProvider.sparkSession

  val logger = LoggerProvider.logger


  def runJob():Unit

  def process(args: Array[String]): Unit = {
    try {
      PropertyConfigurator.configure(getClass.getClassLoader.getResource(LOG4J_PROPERTIES_PATH))
      runJob()
    } catch {
      case e: Exception => logger.info(e.getStackTrace.toString)
        sys.exit(1)
    }
  }

}