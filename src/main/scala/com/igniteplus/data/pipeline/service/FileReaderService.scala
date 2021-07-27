package com.igniteplus.data.pipeline.service
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReaderService{
  def readFile(path:String)(implicit spark:SparkSession): DataFrame = {
      val fileDf:DataFrame = spark.read.format("csv").option("header","true").option("inferSchema","true").csv(path)
      fileDf
  }
}
