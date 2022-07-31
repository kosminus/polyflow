package com.github.kosminus.polyflow.providers

import org.slf4j.{Logger, LoggerFactory}

object LoggerProvider {
     lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
}