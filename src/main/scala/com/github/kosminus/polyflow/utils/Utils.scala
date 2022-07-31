package com.github.kosminus.polyflow.utils

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

object Utils {
  def isLocal():Boolean = {
     System.getProperty("os.name").toLowerCase() match {
       case value:String if value.startsWith("mac") || value.startsWith("windows")  => true
       case _ => false
    }
  }

  def parseConfig(settingsPath:String): Config = {
    if (isLocal()) ConfigFactory.parseResources(settingsPath).resolve()
    else ConfigFactory.parseFile(new File(settingsPath)).resolve()
  }
}
