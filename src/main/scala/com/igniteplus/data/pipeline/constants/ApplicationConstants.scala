package com.igniteplus.data.pipeline.constants

/*IMPORTS*/
import com.igniteplus.data.pipeline.util.ApplicationUtil.{createSparkSession, getSparkConf}

object ApplicationConstants {
  val SPARK_CONF_FILE_NAME = "spark.conf"
  //SPARK SESSION
  val SPARK_CONF = getSparkConf(SPARK_CONF_FILE_NAME)
  implicit val spark = createSparkSession(SPARK_CONF)

  //READ
  val FILE_TYPE:String = "csv"

  //LOCATIONS
  val INPUT_LOCATION_CLICKSTREAM:String = "data/Input/clickstream/clickstream.csv"
  val INPUT_LOCATION_ITEM:String = "data/Input/item/item_data.csv"
  val NULL_VALUES_PATH:String = "data/Input/null-values/null_values.csv"
  val FILE_PATH_TO_WRITE_EXCEPTIONS = "data/Output/pipeline-failures/exceptions.txt"

  //PARAMETERS TO FUNCTIONS

  //PARAMETERS FOR VALIDATED DATA
  val columnToBeValidated_Date:String = "event_timestamp"
  val formatYouWantIn_Date:String = "M/dd/yyyy H:mm"
  val castTo:String = "timestamp"

  //PARAMETERS FOR consistentNamesDf
  val columnToBeModified:String = "redirection_source"
  val columnToBeNamed:String = "redirection_source"

  //PARAMETERS FOR TRIM_FUNCTION
  val columnToBeTrimmed:String = "event_timestamp"

  //PARAMETERS FOR DE_DUPLICATION
 // val SEQ_CLICKSTREAM_PRIMARY_KEYS:Seq[String] = Seq("session_id","item_id")
  //val SEQ_ITEM_PRIMARY_KEYS:Seq[String] = Seq("item_id")

  val toOrderBy:String = "event_timestamp"
  val refColumn:String = "rowNumber"
  val filterExp:String = "rowNumber==1"
  //val toPartitionBy:Seq[String] = SEQ_CLICKSTREAM_PRIMARY_KEYS


  //WRITER
  val FILE_TYPE_WRITE= "csv"

  //PARAMETERS FOR NULL_VALUES_CHECK_AND_REMOVE
  val PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA = Seq("session_id","item_id")
  val PRIMARY_KEY_COLUMNS_ITEM_DATA = Seq("item_id")

}
