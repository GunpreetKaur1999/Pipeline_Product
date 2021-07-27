package com.igniteplus.data.pipeline.transform
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
object dataTypeValidation {

  def dataTypeValidation(df:DataFrame)(implicit spark:SparkSession): DataFrame = {
    /*======================================================Why didn't these work?===================================================*/
    //val dateValidated: DataFrame = clickStreamData.withColumn("DATE",to_date(col("event_timestamp"),"dd-mm-yyyy H.mm"))
    //val dateValidated = clickStreamData.withColumn("Date",to_date(unix_timestamp(clickStreamData.col("event_timestamp"), "dd-MM-yyyy H.mm").cast("timestamp")))
    //val dateValidated = clickStreamData.withColumn("Date",col("event_timestamp").cast("timestamp").cast("date"))
    //val dateValidated = clickStreamData.withColumn("event_timestamp",to_timestamp(col("event_timestamp"),"dd/M/yyyy H:mm"))
    //val dateValidated = clickStreamData.withColumn("Date",to_date(col("event_timestamp"),"YYYY-MM-DD HH24:MM:SS"))
    /*===========================================================FINAL===============================================================*/
    //METHOD 1
    val dateValidated = df
                      .withColumn("event_timestamp",to_timestamp(col("event_timestamp"), "M/dd/yyyy H:mm")
                      .cast("timestamp"))
    dateValidated
    //METHOD 2
//    val dataValidated=df
//      .withColumn("event_timestamp",unix_timestamp(col("event_timestamp"),"MM/dd/yyyy H:mm")
//        .cast("double")
//        .cast("timestamp"))
//      dateValidated

  }

}
