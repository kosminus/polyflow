package com.github.kosminus.polyflow.app

import com.github.kosminus.polyflow.config.PolyConfig
import com.github.kosminus.polyflow.context.EnvContext
import com.github.kosminus.polyflow.model.TopicAndData
import com.github.kosminus.polyflow.providers.SparkSessionProvider
import org.apache.spark.sql.DataFrame

trait IApp extends Serializable {
  @transient protected lazy val sparkSession = SparkSessionProvider.sparkSession
  lazy val envContext: EnvContext = PolyConfig.context

  def run(input:Iterable[TopicAndData[DataFrame]]): Unit
}
