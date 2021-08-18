package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.exception.FileReadException
import com.sun.corba.se.impl.activation.ServerMain.logError
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile


object FileReaderService{

  /**
   *READS THE DATA
   * @param path to read file from
   * @param fileFormat like csv,txt,json
   * @param spark
   * @return is successful a dataframe with data else empty dataframe
   */
  def readFile(path:String, fileFormat:String, writeOutputToPath:String)(implicit spark:SparkSession): DataFrame = {
    val fileDf: DataFrame = {
      try{
          spark.read.format(fileFormat).option("header", "true").option("inferSchema", "true").load(path)
         }
      catch{
        case e: Exception =>
          logError("Unable to read files ")
          spark.emptyDataFrame
      }
    }

    val dfDataCount: Long = fileDf.count()
      if(dfDataCount == 0) {
        throw FileReadException("No files read from the file reader ")
    }

    writeFile(fileDf,fileFormat,writeOutputToPath)
    fileDf
  }
}
