package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import java.io.{BufferedWriter, FileWriter, PrintWriter}

object FileWriterService {
  /**
   * WRITING EXCEPTIONS TO A SEPARATE FILE
   * @param exception message
   * @param filePath to which we have to write the exceptions
   */
  def writeExceptions(exception : String, filePath : String) = {
      val timeStamp = new SimpleDateFormat("dd/MM/yyyy_HH:mm:ss").format(Calendar.getInstance.getTime)
      val out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))
      out.println(timeStamp + " " + exception)
      out.close()
    }

  /**
   * WRITE FUNCTION
   * @param df
   * @param fileType
   * @param filePath
   * @return Just writes the file at the desired location in a desired format specified in params
   */

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

}
