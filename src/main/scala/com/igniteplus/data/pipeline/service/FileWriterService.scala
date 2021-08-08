package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.DataFrame

import java.io.PrintWriter

object FileWriterService {

  def writeFile(df:DataFrame,fileType:String,filePath:String) = {
    try {
      df.write
        .format(fileType)
        .option("path", filePath)
        .mode("overwrite")
        .save()
    }
    catch {
      case e: Exception =>
        FileWriteException("Error with writing the files to the specified location." )
    }
  }

  def writeExceptions(exception : String, fileType : String, filePath : String) = {
    new PrintWriter(filePath)
    {
      write(exception)
      close
    }
  }




}
