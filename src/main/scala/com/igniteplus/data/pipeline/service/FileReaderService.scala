package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.exception.FileReadException
import com.sun.corba.se.impl.activation.ServerMain.logError
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReaderService{
  def readFile(path:String, fileFormat:String)(implicit spark:SparkSession): DataFrame = {

    val fileDf: DataFrame =
      try{
          spark.read.format(fileFormat).option("header", "true").option("inferSchema", "true").load(path)
      }
    catch {
        case e: Exception =>
          logError("unable to read files in the given location")
          spark.emptyDataFrame
      }

    val dfDataCount: Long = fileDf.count()

    if(dfDataCount == 0) {
      throw FileReadException("No files read from the file reader ")
    }
    fileDf
  }
}
