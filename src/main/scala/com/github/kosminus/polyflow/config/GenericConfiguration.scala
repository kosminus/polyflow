package com.github.kosminus.polyflow.config

case class GenericConfiguration(appName:String,
                            topic:String,
                            format:String,
                            paths: Option[Seq[String]],
                            options: Option[Map[String,String]]) 

