package com.github.kosminus.polyflow.config

case class KafkaConfiguration(topics: List[String],
                              kafkaBootstrapServers: String,
                              startingOffsets : Option[String],
                              failOnDataLoss : Option[String],
                              kafkaConsumerPollTimeoutMs: Option[String],
                              fetchOffsetNumRetries : Option[String],
                              maxOffsetRetryIntervalMs : Option[String],
                              unipValue : Option[Boolean],
                              castValue : Option[String])
