package com.github.kosminus.polyflow.config

import java.sql.Timestamp

case class KafkaInput (key: Array[Byte],
                      value: Array[Byte],
                      topic: String,
                      partition:Int,
                      offset:Long,
                      timestamp:Timestamp,
                      timestampType:Int)