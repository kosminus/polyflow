package com.github.kosminus.polyflow.config

trait SinkConfiguration

case class ConsoleSinkConfiguration(format: String,
                                    numRows: Option[Int],
                                    truncate: Option[Boolean],
                                    outputMode: Option[String]) extends SinkConfiguration

case class KafkaSinkConfiguration(bootstrapServers: String,
                                  topic: String,
                                  separator: String,
                                  checkpointLocation: String,
                                  configuration: Option[Map[String, String]],
                                  selectExprKey: Option[String],
                                  selectExprValue: Option[String]) extends SinkConfiguration

case class HiveSinkConfiguration(format: String,
                                 dbName: Option[String],
                                 saveMode: Option[String],
                                 tableName: String,
                                 partitionBy: Option[Seq[String]],
                                 options: Option[Map[String, String]]) extends SinkConfiguration

case class CassandraSinkConfiguration(format: String,
                                      user: String,
                                      password: String,
                                      table: String,
                                      keyspace: String,
                                      saveMode: Option[String],
                                      options: Option[Map[String, String]]) extends SinkConfiguration

case class BigQuerySinkConfiguration(format: String,
                                     project: String,
                                     dataset: String,
                                     table: String,
                                     temporaryGcsBucket: String,
                                     saveMode: Option[String]) extends SinkConfiguration

case class GenericSinkConfiguration(format: String,
                                    options: Map[String, String],
                                    partitionBy: Option[Seq[String]],
                                    saveMode: Option[String],
                                    path: String) extends SinkConfiguration

case class JDBCSinkConfiguration(url: String,
                                 driver: String,
                                 user: String,
                                 password: String,
                                 dbTable: String,
                                 mode: Option[String],
                                 options: Option[Map[String, String]]) extends SinkConfiguration

