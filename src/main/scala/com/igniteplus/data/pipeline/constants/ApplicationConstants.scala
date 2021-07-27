package com.igniteplus.data.pipeline.constants

import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transform.dataTypeValidation.dataTypeValidation
import com.igniteplus.data.pipeline.transform.deDuplication.deDuplication
import org.apache.spark.sql.DataFrame
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.functions.col

object ApplicationConstants {
  //SPARK SESSION
  val APP_NAME = "product"
  val MASTER_NAME = "local"
  val spark = createSparkSession(APP_NAME,MASTER_NAME)

  //LOCATIONS
  val INPUT_LOCATION_CLICKSTREAM = "data/Input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM = "data/Input/item/item_data.csv"

  //DATAFRAMES
  val CLICKSTREAM_DATA: DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM)(spark)
  val ITEM_DATA:DataFrame = readFile(INPUT_LOCATION_ITEM)(spark)
  val VALIDATED_DATA: DataFrame = dataTypeValidation(CLICKSTREAM_DATA)(spark)
  val DEDUPLICATED_DATA: DataFrame = deDuplication(VALIDATED_DATA)(spark)

  //COLUMNS




}
