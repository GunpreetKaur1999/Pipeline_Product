import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.exception.FileReadException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class FileReaderServiceTest1 extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  test("Reading the file") {
    val sampleDF = readFile(INPUT_LOCATION_CLICKSTREAM, FILE_TYPE, WRITE_TEST_OUTPUT)(spark)
    val rcount = sampleDF.count()
    assert(rcount > 0, "Count must be greater than 0")
  }

  test("throw exception in case it's not able to read data")  {
    assertThrows[FileReadException] {
      val sampleDF = readFile(readWrongLocation, fileFormat, writeOutputPath)(spark)
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}

class FileReaderServiceTest2 extends AnyFlatSpec with BeforeAndAfterAll{
  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  "readFile() method" should "read data from the given location" in {
    val sampleDF = readFile(readLocation,fileFormat,writeOutputPath)(spark)
    val rcount = sampleDF.count()
    assertResult(countShouldBe)(rcount)
  }

  "readFile() method" should "throw exception in case it's not able to read data" in {
    assertThrows[FileReadException] {
      val sampleDF = readFile(readWrongLocation, fileFormat, writeOutputPath)(spark)
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
