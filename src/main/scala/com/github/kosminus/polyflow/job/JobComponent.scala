package com.github.kosminus.polyflow.job

import com.github.kosminus.polyflow.Launcher.applicationPipeline.envContext
import com.github.kosminus.polyflow.app.AppComponent
import com.github.kosminus.polyflow.config.{GenericConfiguration, PolyConfig}
import com.github.kosminus.polyflow.exception.MissingConfigurationException
import com.github.kosminus.polyflow.model.TopicAndData
import com.github.kosminus.polyflow.reader.InputFactory
import org.apache.spark.sql.DataFrame

trait JobComponent {
  this: AppComponent =>

  val job: IJob

  class StructuredStreamingJob extends IJob {
    override def runJob(): Unit = {
      logger.info("===== StreamingJob started =====")
      PolyConfig.job match {
        case Right(kafkaConf) =>
          logger.info(kafkaConf.toString)

          val input : Iterable[TopicAndData[DataFrame]] = InputFactory.getDataKafka(kafkaConf)
            app.run(input)

        case Left(genericConfiguration) =>
          println(genericConfiguration.toString)

          val input : Iterable[TopicAndData[DataFrame]] = InputFactory.getDataGeneric(genericConfiguration,envContext)
          app.run(input)

      }
    }
  }

  class BatchJob extends IJob {
    override def runJob(): Unit = {
      logger.info("===== BatchJob started =====")
      PolyConfig.job match {
        case Left(genericConfiguration: GenericConfiguration) =>
          logger.info(genericConfiguration.toString)
          val input : Iterable[TopicAndData[DataFrame]] = InputFactory.getDataGeneric(genericConfiguration,envContext)
          app.run(input)
        case _ => throw MissingConfigurationException("no good job config")

      }

    }

  }


}
