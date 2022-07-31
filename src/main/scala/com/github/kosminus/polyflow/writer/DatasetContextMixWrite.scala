package com.github.kosminus.polyflow.writer

import com.github.kosminus.polyflow.Launcher.applicationPipeline.envContext
import com.github.kosminus.polyflow.config.{BigQuerySinkConfiguration, CassandraSinkConfiguration, ConsoleSinkConfiguration, GenericSinkConfiguration, HiveSinkConfiguration, JDBCSinkConfiguration, KafkaSinkConfiguration}
import org.apache.spark.sql.Dataset
import com.github.kosminus.polyflow.config.Sink.HSink
import com.github.kosminus.polyflow.context.BatchContext

object DatasetContextMixWrite {

  implicit class ExtensionWriter[T] (val dataset: Dataset[T]) extends Serializable {
      def hSinkWrite(sinkConfiguration: HSink):Unit =

        sinkConfiguration.toList.flatten.foreach {
          case hiveSinkConfiguration: HiveSinkConfiguration => polyWrite.apply(hiveSinkConfiguration,dataset,envContext)
          case consoleSinkConfiguration: ConsoleSinkConfiguration => polyWrite.apply(consoleSinkConfiguration,dataset,envContext)
          case kafkaSinkConfiguration: KafkaSinkConfiguration => polyWrite.apply(kafkaSinkConfiguration,dataset,envContext)
          case cassandraSinkConfiguration: CassandraSinkConfiguration => polyWrite.apply(cassandraSinkConfiguration,dataset,envContext)
          case bigQuerySinkConfiguration: BigQuerySinkConfiguration => polyWrite.apply(bigQuerySinkConfiguration,dataset,envContext)
          case genericSinkConfiguration: GenericSinkConfiguration => polyWrite.apply(genericSinkConfiguration,dataset,envContext)
          case jdbcSinkConfiguration: JDBCSinkConfiguration => polyWrite.apply(jdbcSinkConfiguration,dataset,envContext)

        }
      }
}


