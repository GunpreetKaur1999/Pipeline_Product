package com.igniteplus.data.pipeline
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_DATA, DEDUPLICATED_DATA, INPUT_LOCATION_CLICKSTREAM, INPUT_LOCATION_ITEM, ITEM_DATA, VALIDATED_DATA, spark}
import com.igniteplus.data.pipeline.transform.consistentNaming.consistentNaming
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col


object ProductName {
 def main(args: Array[String]): Unit = {
//    println("Reading Clickstream Data:")
//    CLICKSTREAM_DATA.show()
//    println("Reading Item Data:")
//    ITEM_DATA.show()
//    println("Schema of clickstream data before datatype validation:")
//    CLICKSTREAM_DATA.printSchema()
//    println("Schema of item Data:")
//    ITEM_DATA.printSchema()
//    println("Schema of clickstream Data after datatype validation:")
//    VALIDATED_DATA.printSchema()
//    println("Clickstream Data after de-duplication:")
//    DEDUPLICATED_DATA.show()
//    println("Number of rows before de-duplication:"+VALIDATED_DATA.count())
//    println("Number of rows after de-duplication:"+DEDUPLICATED_DATA.count())
      val df = consistentNaming(DEDUPLICATED_DATA)
      df.show()
  }
}
