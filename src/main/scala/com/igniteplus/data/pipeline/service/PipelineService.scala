package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.DataPipeline.logger
import com.igniteplus.data.pipeline.constants.ApplicationConstants
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.TransformationsOnData.Cleanser.Cleanser._
import com.igniteplus.data.pipeline.TransformationsOnData.TranformingTheData.TransformationOfData.{consistentNaming, dataTypeValidation}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.DataFrame



object PipelineService {

  def pipelineService() = {

    //IMPLICIT VALUE OF SPARK
    implicit val spark = createSparkSession(SPARK_CONF)

    /*READING OF CLICK-STREAM DATA*/
    val clickStreamDataDf : DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM, FILE_TYPE, WRITE_OUTPUT_TO_PATH_CLICKSTREAM)
    /*READING OF ITEM DATA*/
    val itemDataDf : DataFrame = readFile(INPUT_LOCATION_ITEM, FILE_TYPE, WRITE_OUTPUT_TO_PATH_ITEM)


    /*NULL VALUE CHECKING*/
    val nullValueCheckInClickStreamDf : DataFrame = nullValueCheckAndRemove(clickStreamDataDf,PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA,WRITE_OUTPUT_FORMAT,WRITE_OUTPUT_TO_PATH_NOT_NULL_CLICKSTREAM,WRITE_OUTPUT_TO_PATH_NULL_CLICKSTREAM)
    val nullValueCheckInItemDf : DataFrame = nullValueCheckAndRemove(itemDataDf,PRIMARY_KEY_COLUMNS_ITEM_DATA,WRITE_OUTPUT_FORMAT,WRITE_OUTPUT_TO_PATH_NOT_NULL_ITEM,WRITE_OUTPUT_TO_PATH_NULL_ITEM)

    /*REMOVAL OF DEDUPLICATED DATA*/
    val deDuplicatedDf: DataFrame = deDuplication(nullValueCheckInClickStreamDf, filterExp, refColumn, PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA,toOrderBy,WRITE_OUTPUT_FORMAT,WRITE_OUTPUT_TO_PATH_DEDUPLICATED_DATA_CLICKSTREAM)
    val deDuplicatedDfItemDF: DataFrame = deDuplication(nullValueCheckInItemDf, filterExp, refColumn, PRIMARY_KEY_COLUMNS_ITEM_DATA,None,WRITE_OUTPUT_FORMAT,WRITE_OUTPUT_TO_PATH_DEDUPLICATED_DATA_ITEM)

    deDuplicatedDf.show()

    /*DATE TYPE VALIDATION*/
    val validatedDataDf: DataFrame = dataTypeValidation(clickStreamDataDf, columnToBeValidated_Date, formatYouWantIn_Date, castTo)
    validatedDataDf.show()

    scala.io.StdIn.readLine()


  /*  /*NULL VALUES REMOVAL AND WRITING THEM TO A FILE*/
    val nullValuesRemovedClickStreamDf: DataFrame = nullValuesCheckAndRemove(validatedDataDf, clickstream_columns_check_NULL, FILE_TYPE_WRITE, NULL_VALUES_PATH)
    val nullValuesRemovedItemDf: DataFrame = nullValuesCheckAndRemove(itemDataDf, item_columns_check_NULL, FILE_TYPE_WRITE, NULL_VALUES_PATH)





    /*CONSISTENT NAMES*/
    val consistentNamesDf: DataFrame = consistentNaming(deDuplicatedDf, columnToBeModified, columnToBeNamed)


    /*TRIMMING OF SPACES*/
    val trimmedDataDf: DataFrame = trimFunction(consistentNamesDf, columnToBeTrimmed) */

  }
}
