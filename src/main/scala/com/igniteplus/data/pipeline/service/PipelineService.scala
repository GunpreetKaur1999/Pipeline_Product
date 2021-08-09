package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{FILE_TYPE, FILE_TYPE_WRITE, INPUT_LOCATION_CLICKSTREAM, INPUT_LOCATION_ITEM, NULL_VALUES_PATH, PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA, PRIMARY_KEY_COLUMNS_ITEM_DATA, SPARK_CONF, castTo, columnToBeModified, columnToBeNamed, columnToBeTrimmed, columnToBeValidated_Date, filterExp, formatYouWantIn_Date, refColumn, toOrderBy}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transform.Cleanser.{nullValueCheckAndRemove}
import com.igniteplus.data.pipeline.transform.TransformationOfData.{consistentNaming, dataTypeValidation}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.DataFrame



object PipelineService {

  def pipelineService() = {

    //IMPLICIT VALUE OF SPARK
    implicit val spark = createSparkSession(SPARK_CONF)

    /*READING OF CLICK-STREAM DATA*/
    val clickStreamDataDf : DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM, FILE_TYPE)
    /*READING OF ITEM DATA*/
    val itemDataDf : DataFrame = readFile(INPUT_LOCATION_ITEM, FILE_TYPE)


    /*NULL VALUE CHECKING*/
    val nullValueCheckInClickStreamDf : DataFrame = nullValueCheckAndRemove(clickStreamDataDf,PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA)
    val nullValueCheckInItemDf : DataFrame = nullValueCheckAndRemove(itemDataDf,PRIMARY_KEY_COLUMNS_ITEM_DATA)










 /*   /*DATE TYPE VALIDATION*/
    val validatedDataDf: DataFrame = dataTypeValidation(clickStreamDataDf, columnToBeValidated_Date, formatYouWantIn_Date, castTo)


    /*NULL VALUES REMOVAL AND WRITING THEM TO A FILE*/
    val nullValuesRemovedClickStreamDf: DataFrame = nullValuesCheckAndRemove(validatedDataDf, clickstream_columns_check_NULL, FILE_TYPE_WRITE, NULL_VALUES_PATH)
    val nullValuesRemovedItemDf: DataFrame = nullValuesCheckAndRemove(itemDataDf, item_columns_check_NULL, FILE_TYPE_WRITE, NULL_VALUES_PATH)


    /*REMOVAL OF DEDUPLICATED DATA*/
    val deDuplicatedDf: DataFrame = deDuplication(validatedDataDf, toOrderBy, filterExp, refColumn, SEQ_CLICKSTREAM_PRIMARY_KEYS: _*)


    /*CONSISTENT NAMES*/
    val consistentNamesDf: DataFrame = consistentNaming(deDuplicatedDf, columnToBeModified, columnToBeNamed)


    /*TRIMMING OF SPACES*/
    val trimmedDataDf: DataFrame = trimFunction(consistentNamesDf, columnToBeTrimmed) */
  }
}
