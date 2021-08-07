package com.igniteplus.data.pipeline
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.PipelineService
import org.apache.log4j.Logger

object DataPipeline {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting")
     try {
       PipelineService.pipelineService()
     }

     catch {
       case fr: FileReadException =>
         println("File Reader")

       case fw: FileWriteException =>
         println("File Writter")

       case e: Exception =>
         println("Unknown exception")
     }
    logger.info("Ending")
   }
}
