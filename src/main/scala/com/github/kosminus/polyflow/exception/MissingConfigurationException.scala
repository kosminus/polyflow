package com.github.kosminus.polyflow.exception

class MissingConfigurationException private(ex: RuntimeException) extends RuntimeException(ex) {
  def this(message: String) = this(new RuntimeException(message))

  def this(message: String, throwable: Throwable) = this(new RuntimeException(message, throwable))
}

object MissingConfigurationException {
  def apply(message: String) = new MissingConfigurationException(message)

  def apply(message: String, throwable: Throwable) = new MissingConfigurationException(message, throwable)
}
