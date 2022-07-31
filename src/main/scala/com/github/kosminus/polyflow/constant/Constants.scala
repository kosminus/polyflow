package com.github.kosminus.polyflow.constant

object Constants {
  val APPLICATION_CONFIG_PATH = "application.conf"
  val SPARK = "Spark"
  val TRANSFER= "Transfer"
  val SINK = "Sink"
  val LOG4J_PROPERTIES_PATH = "log4j.properties"
  val JOB = "Job"
  val SPARK_ENV_CONTEXT = "Context"
  val PARTNER = "Partner"
  val KAFKA_JSON_VALUES = "to_json(struct(*)) AS value"
  val KAFKA_KEY_NULL_VALUE = "null"
  val APPEND = "append"
  val KAFKA = "kafka"
  val TOPIC_KEY = "topic"
  val EVENT_VALUE_KEY = "value"
  val EVENT_BODY_KEY = "body"

  object SinkType {
    val console = "Console"
    val hive = "Hive"
    val generic = "Generic"
    val kafka = "Kafka"
    val jdbc = "Jdbc"
    val bigquery = "BigQuery"
    val cassandra = "Cassandra"
  }

  object SparkMaster {
    val LOCAL_MODE = "local[*]"
  }

}
