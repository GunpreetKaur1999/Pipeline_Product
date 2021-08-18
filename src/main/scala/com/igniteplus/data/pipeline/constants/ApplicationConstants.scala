package com.igniteplus.data.pipeline.constants

/*IMPORTS*/
import com.igniteplus.data.pipeline.util.ApplicationUtil.{createSparkSession, getSparkConf}

object ApplicationConstants {

  //SPARK SESSION
  val SPARK_CONF_FILE_NAME = "spark.conf"
  val SPARK_CONF = getSparkConf(SPARK_CONF_FILE_NAME)
  implicit val spark = createSparkSession(SPARK_CONF)

  //READ
  val FILE_TYPE:String = "csv"
  val WRITE_OUTPUT_TO_PATH_CLICKSTREAM : String = "data/Output/merged-data/readOutput/readOutputClickStream.csv"
  val WRITE_OUTPUT_TO_PATH_ITEM : String = "data/Output/merged-data/readOutput/readOutputItem.csv"

  //LOCATIONS
  val INPUT_LOCATION_CLICKSTREAM : String = "data/Input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM : String = "data/Input/item/item_data.csv"
  val NULL_VALUES_PATH : String = "data/Input/null-values/null_values.csv"
  val FILE_PATH_TO_WRITE_EXCEPTIONS = "data/Output/pipeline-failures/exceptions.txt"

  //PARAMETERS TO FUNCTIONS
  //PARAMETERS FOR NULL_VALUES_CHECK_AND_REMOVE
  val PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA : Seq[String] = Seq("session_id","item_id")
  val PRIMARY_KEY_COLUMNS_ITEM_DATA : Seq[String] = Seq("item_id")
  val WRITE_OUTPUT_TO_PATH_NULL_CLICKSTREAM : String = "data/Output/merged-data/nullValues/nullValuesClickStream.csv"
  val WRITE_OUTPUT_TO_PATH_NOT_NULL_CLICKSTREAM : String = "data/Output/merged-data/notNullValues/notNullDataClickStream.csv"
  val WRITE_OUTPUT_TO_PATH_NOT_NULL_ITEM : String = "data/Output/merged-data/notNullValues/notNullDataItem.csv"
  val WRITE_OUTPUT_TO_PATH_NULL_ITEM : String = "data/Output/merged-data/nullValues/nullValuesItems.csv"


  //PARAMETERS FOR DE_DUPLICATION
  val toOrderBy : Option[String] = Some("event_timestamp")
  val refColumn : String = "rowNumber"
  val filterExp : String = "rowNumber==1"
  val WRITE_OUTPUT_FORMAT : String = "csv"
  val WRITE_OUTPUT_TO_PATH_DEDUPLICATED_DATA_CLICKSTREAM : String = "data/Output/merged-data/Deduplicated_Data/deduplicatedDataClickStream.csv"
  val WRITE_OUTPUT_TO_PATH_DEDUPLICATED_DATA_ITEM : String = "data/Output/merged-data/Deduplicated_Data/deDuplicatedDataItem.csv"





  //PARAMETERS FOR VALIDATED DATA
  val columnToBeValidated_Date:String = "event_timestamp"
  val formatYouWantIn_Date:String = "M/dd/yyyy H:mm"
  val castTo:String = "timestamp"

  //PARAMETERS FOR consistentNamesDf
  val columnToBeModified:String = "redirection_source"
  val columnToBeNamed:String = "redirection_source"

  //PARAMETERS FOR TRIM_FUNCTION
  val columnToBeTrimmed:String = "event_timestamp"



  //WRITER
  val FILE_TYPE_WRITE= "csv"

  val WRITE_TEST_OUTPUT = "data/Output/merged-data/testOutput.csv"


}
