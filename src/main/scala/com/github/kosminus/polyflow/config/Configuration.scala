package com.github.kosminus.polyflow.config

import com.github.kosminus.polyflow.constant.Constants._
import com.github.kosminus.polyflow.utils.Utils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

class Configuration(settingsPath:String) {
  val config: Config = Utils.parseConfig(settingsPath)
  val sparkConfiguration: SparkConfiguration = config.as[SparkConfiguration](SPARK)
  val transferConfiguration:TransferConfiguration = config.as[TransferConfiguration](TRANSFER)

}


