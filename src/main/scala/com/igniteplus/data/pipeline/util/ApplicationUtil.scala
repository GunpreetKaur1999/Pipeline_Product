package com.igniteplus.data.pipeline.util
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.util.Properties
import scala.collection.JavaConverters._



object ApplicationUtil{
  /**
   * READING SPARK CONFIGURATIONS FROM A SEPERATE FILE
   * @param fileName where you've mentioned the spark configuration
   * @return sparkConf object
   */
  def getSparkConf(fileName: String): SparkConf ={
    val sparkAppConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile(fileName).bufferedReader())
    props.asScala.foreach(kv => sparkAppConf.set(kv._1,kv._2))
    sparkAppConf
  }

  /**
   * SPARK SESSION CREATION
   * @param sparkConfiguration
   * @return spark
   */
  def createSparkSession(sparkConfiguration:SparkConf):SparkSession = {
    implicit val spark:SparkSession = SparkSession.builder().config(sparkConfiguration).getOrCreate()
    spark
  }

}
