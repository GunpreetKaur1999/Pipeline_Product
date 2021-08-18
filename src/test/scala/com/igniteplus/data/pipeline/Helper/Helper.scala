package com.igniteplus.data.pipeline.Helper

object Helper {

  //tests
  val WRITE_TEST_OUTPUT = "data/Output/merged-data/testOutput.csv"
  val readLocation = "data/Input/testing/fileRead.csv"
  val fileFormat = "csv"
  val writeOutputPath = "data/Output/merged-data/testOutput.csv"
  val countShouldBe = 3
  val readWrongLocation = "data/Input/testing/fileRea.csv"
  val deduplicationLocation = "data/Input/testing/removeDuplicates.csv"
  val writeOutputPathForDeduplication = "data/Output/merged-data/deduplicationTestOutput.csv"
  val PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA : Seq[String] = Seq("session_id","item_id")


}
