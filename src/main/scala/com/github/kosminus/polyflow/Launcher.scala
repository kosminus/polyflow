package com.github.kosminus.polyflow

import com.github.kosminus.polyflow.app.TransferApp
import com.github.kosminus.polyflow.config.{PolyConfig, TransferConfiguration}
import com.github.kosminus.polyflow.engine.TransferEngineComponent
import com.github.kosminus.polyflow.job.JobTypeByContext
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

object Launcher extends JobTypeByContext {
  override val applicationPipeline = new TransferApp with TransferEngineComponent {
    private val configValue = PolyConfig.partner.as[TransferConfiguration]("transfer")
    override val transferEngine: TransferEngine = new TransferEngine {
      override val config: TransferConfiguration = configValue
      override val sqlText: Option[String] = configValue.sqlText
    }
  }
}
