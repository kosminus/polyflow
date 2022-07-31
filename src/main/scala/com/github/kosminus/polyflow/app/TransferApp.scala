package com.github.kosminus.polyflow.app

import com.github.kosminus.polyflow.engine.TransferEngineComponent
import com.github.kosminus.polyflow.model.TopicAndData
import com.github.kosminus.polyflow.writer.DatasetContextWrite.Extension
import com.github.kosminus.polyflow.writer.SinkFactory
import org.apache.spark.sql.DataFrame


class TransferApp extends IApp {
  this: TransferEngineComponent =>

  override def run(input: Iterable[TopicAndData[DataFrame]]): Unit = {
    input.foreach { topicAndData =>
      val topic = topicAndData.topic
      val data = topicAndData.data


      val sinkConfiguration = SinkFactory.getConfiguration(topic)
       transferEngine.transfer(data).contextWrite(sinkConfiguration)

    }
  }
}
