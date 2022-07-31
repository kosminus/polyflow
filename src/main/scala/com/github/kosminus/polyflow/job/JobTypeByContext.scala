package com.github.kosminus.polyflow.job

import com.github.kosminus.polyflow.app.IApp
import com.github.kosminus.polyflow.config.{CoContext, Configuration, PolyConfig}
import com.github.kosminus.polyflow.context.{BatchContext, StreamingContext}
import com.github.kosminus.polyflow.providers.LoggerProvider
import shapeless.Coproduct

trait JobTypeByContext {
  val logger = LoggerProvider.logger
  protected val applicationPipeline: IApp

  def main(args: Array[String]): Unit = {
    logger.info(" ==== JOB STARTED ====")

    val job = PolyConfig.context match {
      case context: BatchContext =>
        logger.info("batch context")
        val ctx = Coproduct[CoContext](context,applicationPipeline,args)
        ctx map PolyJobType

      case context: StreamingContext =>
        logger.info("streaming context")
        val ctx = Coproduct[CoContext](context,applicationPipeline,args)
        ctx map PolyJobType

      case _ =>
        throw new RuntimeException("could not find job for ")
    }


    logger.info(" ==== JOB ENDED ====")

  }

}
