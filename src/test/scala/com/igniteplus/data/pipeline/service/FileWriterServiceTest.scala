package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class FileWriterServiceTest extends AnyFlatSpec{

    @transient var spark: SparkSession = _

    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()


    val testDf : DataFrame = readFile("data/Input/testing/fileRead.csv","csv","data/Output/testOutput/testDataOutput.csv")(spark)

    "writeFile() method" should "write data to the given location" in {
      val sampleDF = writeFile(testDf,"csv","data/Output/testOutput/testDataOutput.csv")
      val readSampleOutputDf = readFile("data/Output/testOutput/testDataOutput.csv","csv","data/Output/testOutput/writingOutput.csv")(spark)
      val countShouldBe = 3
      val checkOutputFile = readSampleOutputDf.count()
      assertResult(countShouldBe)(checkOutputFile)
    }
}
