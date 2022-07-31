package com.github.kosminus.polyflow.config

case class SparkConfiguration(appName:String,
                              checkpointDirectory:String,
                              confSettings:Option[Map[String,String]],
                              temporaryTableName:Option[String])
