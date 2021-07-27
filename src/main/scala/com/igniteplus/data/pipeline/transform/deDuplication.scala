package com.igniteplus.data.pipeline.transform
import com.igniteplus.data.pipeline.constants.ApplicationConstants.INPUT_LOCATION_CLICKSTREAM
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.{DataFrame, SparkSession}

object deDuplication {
  //METHOD 1
  def deDuplication(Df:DataFrame)(implicit spark:SparkSession): DataFrame = {
    val clickStreamData: DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM)(spark)
    val dropped = clickStreamData.dropDuplicates("device_type", "session_id", "visitor_id", "item_id")
    dropped
  }
//  //METHOD 2
//  def deDuplicationClickStream(df:DataFrame):DataFrame= {
//    val winSpec = Window.partitionBy("session_id", "item_id").orderBy(desc("event_timestamp"))
//    val duplicate: DataFrame = df.withColumn("row_number", row_number().over(winSpec))
//    val deDuplicate: DataFrame = duplicate.filter("row_number==1").drop("row_number")
//    deDuplicate
//  }

}
