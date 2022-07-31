package com.github.kosminus.polyflow.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Try}

trait ReaderUtils {

  def readData(format: String,
               options: Option[Map[String, String]],
               multiplePaths: Traversable[String],
               f: (String, Option[Map[String, String]], Option[String]) => DataFrame): DataFrame = {
    if (multiplePaths.isEmpty)
      f(format, options, None)
    else
      read(format,
        options,
        multiplePaths.tail,
        f(format, options, Some(multiplePaths.head)),
        f
      )
  }

  @tailrec
  private def read(format: String,
                   options: Option[Map[String, String]],
                   multiplePaths: Traversable[String],
                   dataFrame: DataFrame,
                   f: (String, Option[Map[String, String]], Option[String]) => DataFrame): DataFrame = {
    if (multiplePaths.isEmpty)
      dataFrame
    else
      read(format,
        options,
        multiplePaths.tail,
        dataFrame.union(f(format, options, Some(multiplePaths.head))),
        f)

  }

  def readData(sparkSession: SparkSession,
               list: Traversable[Any],
               f: (SparkSession, Traversable[Any]) => DataFrame): Try[DataFrame] = {
    if (list.isEmpty)
      Failure(new Exception("invalid input"))
    else
      Try(read(list, f.apply(sparkSession, list), sparkSession, f))
  }

  @tailrec
  private def read(list: Traversable[Any],
                   dataFrame: DataFrame,
                   sparkSession: SparkSession,
                   f: (SparkSession, Traversable[Any]) => DataFrame): DataFrame = {
    if (list.tail.isEmpty)
      dataFrame
    else
      read(list.tail, dataFrame.union(f.apply(sparkSession, list.tail)), sparkSession, f)
  }
}