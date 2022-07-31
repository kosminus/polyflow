package com.github.kosminus.polyflow.context

sealed trait EnvContext

case class BatchContext() extends EnvContext
case class StreamingContext() extends EnvContext
