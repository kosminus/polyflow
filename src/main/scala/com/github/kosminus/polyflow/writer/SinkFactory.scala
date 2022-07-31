package com.github.kosminus.polyflow.writer

import com.github.kosminus.polyflow.config.PolyConfig
import com.github.kosminus.polyflow.config.Sink.HSink
import com.github.kosminus.polyflow.exception.MissingConfigurationException

import scala.util.{Failure, Success, Try}

object SinkFactory {
  def getConfiguration(sinkIdentification: String): HSink = {
    Try(PolyConfig.sink.get(sinkIdentification)) match {
      case Success(hSinkOption: Option[HSink]) => hSinkOption match {
        case Some(hSink) =>
          hSink
        case None => throw  MissingConfigurationException("please check sink configuration")
      }
      case Failure(ex) => throw  MissingConfigurationException("please check sink configuration")

    }

  }
}
