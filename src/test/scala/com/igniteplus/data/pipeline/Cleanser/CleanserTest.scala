package com.igniteplus.data.pipeline.Cleanser

import com.igniteplus.data.pipeline.Helper.Helper.{PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA, countShouldBe, deduplicationLocation, fileFormat, readLocation, readWrongLocation, writeOutputPath, writeOutputPathForDeduplication}
import com.igniteplus.data.pipeline.TransformationsOnData.Cleanser.Cleanser.removeDuplicates
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CleanserTest extends AnyFlatSpec with BeforeAndAfterAll{


    @transient var spark: SparkSession = _

    override def beforeAll(): Unit = {
      spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
    }

    "removeDuplicates() method" should "remove the duplicates from the inputDF" in {
      val sampleDF = readFile(deduplicationLocation, fileFormat, writeOutputPathForDeduplication)(spark)
      val deDuplicatedDF = removeDuplicates(sampleDF,PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA,Some("event_timestamp"))
      val rcount = deDuplicatedDF.count()
      val expectedCount = 2
      assertResult(expectedCount)(rcount)
    }


    override def afterAll(): Unit = {
      spark.stop()
    }


}
