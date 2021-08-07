package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.DataFrame

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
        FileWriteException("unable to write files in the given location " )
    }
  }
}
