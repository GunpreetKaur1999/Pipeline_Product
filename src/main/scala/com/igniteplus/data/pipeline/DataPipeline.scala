package com.igniteplus.data.pipeline
import com.igniteplus.data.pipeline.constants.ApplicationConstants.FILE_PATH_TO_WRITE_EXCEPTIONS
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.PipelineService
import com.igniteplus.data.pipeline.service.FileWriterService.writeExceptions
import com.sun.corba.se.impl.activation.ServerMain.logError
import com.sun.org.slf4j.internal
import com.sun.org.slf4j.internal.LoggerFactory


object DataPipeline {

  val logger :internal.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    try {
       PipelineService.pipelineService()
     }
     catch {
       case fr: FileReadException =>
         writeExceptions("File Reader exception " + fr,FILE_PATH_TO_WRITE_EXCEPTIONS)

       case fw: FileWriteException =>
         logError("File Writer")

       case e: Exception =>
         logError("Unknown exception")
     }
  }
}
