import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.constants.ApplicationConstants._

class FileReaderServiceTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark : SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  test("Reading the file"){
    val sampleDF = readFile(INPUT_LOCATION_CLICKSTREAM,FILE_TYPE,WRITE_TEST_OUTPUT)(spark)
    val rcount = sampleDF.count()
    assert(rcount>0, "Count must be greater than 0")
  }




  override def afterAll(): Unit = {
    spark.stop()
  }

}
