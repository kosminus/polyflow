#!/usr/bin/env bash

spark_master="yarn"
spark_deploy_mode="cluster"
spark_driver_memory="2G"
spark_num_executors=3
spark_executor_memory="3G"
spark_executor_memory_overhead="1G"
spark_executor_cores=2
application_jar="polyflow-1.0.0"
job_class="com.github.kosminus.polyflow.Launcher"

spark-submit \
      --master ${spark_master} \
      --deploy-mode ${spark_deploy_mode} \
      --driver-memory ${spark_driver_memory} \
      --num-executors ${spark_num_executors} \
      --executor-cores ${spark_executor_cores} \
      --executor-memory ${spark_executor_memory} \
      --conf spark.executor.memoryOverhead=${spark_executor_memory_overhead} \
      --files ./log4j.properties,./application.conf \
      --class ${job_class} \
      ${application_jar}