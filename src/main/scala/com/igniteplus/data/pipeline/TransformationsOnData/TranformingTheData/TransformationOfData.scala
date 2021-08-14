package com.igniteplus.data.pipeline.TransformationsOnData.TranformingTheData

//import com.igniteplus.data.pipeline.constants.ApplicationConstants.{TIMESTAMP_DATATYPE, TTIMESTAMP_FORMAT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, initcap, to_timestamp, unix_timestamp}
import com.igniteplus.data.pipeline.service.FileWriterService._

object TransformationOfData {

  /*FUNCTION FOR CONSISTENT NAMING*/
  def consistentNaming(df: DataFrame, columnToBeModified: String, columnToBeNamed: String): DataFrame = {
    val consistentNames: DataFrame = df.withColumn(columnToBeNamed, initcap(col(columnToBeModified)))
    consistentNames
  }

  /*FUNCTION FOR DATE DATA TYPE VALIDATION*/
  def dataTypeValidation(df: DataFrame, columnToBeValidated: String, formatYouWantIn_Date: String, castTo: String): DataFrame = {
    val dateValidated = df
      .withColumn(columnToBeValidated, to_timestamp(col(columnToBeValidated), formatYouWantIn_Date)
        .cast(castTo))
    writeFile(df,"csv","data/Output/merged-data/dataTypeValidation_Data/dataTypeValidation.csv")
    dateValidated
  }




}
