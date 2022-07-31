package com.github.kosminus.polyflow.config

import com.github.kosminus.polyflow.config.Sink.HSink
import com.github.kosminus.polyflow.constant.Constants._
import com.github.kosminus.polyflow.context.EnvContext
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

object PolyConfig extends Configuration(settingsPath = APPLICATION_CONFIG_PATH) {
  lazy val job:JobConfiguration = config.as[JobConfiguration](JOB)
  lazy val context:EnvContext = config.as[EnvContext](SPARK_ENV_CONTEXT)
  lazy val sink : Map[String,HSink] = Sink(config,SINK)
  lazy val partner : Config = config.getConfig(PARTNER)
}
