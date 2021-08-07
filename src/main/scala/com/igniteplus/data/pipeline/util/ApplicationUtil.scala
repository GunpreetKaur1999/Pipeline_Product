package com.igniteplus.data.pipeline.util
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source
import java.util.Properties
import scala.collection.JavaConverters._

object ApplicationUtil{

  def getSparkConf(): SparkConf ={
    val sparkAppConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.asScala.foreach(kv => sparkAppConf.set(kv._1,kv._2))
    sparkAppConf
  }

  def createSparkSession(sparkConfiguration:SparkConf):SparkSession = {
    implicit val spark:SparkSession = SparkSession.builder().config(sparkConfiguration).getOrCreate()
    spark
  }
}
