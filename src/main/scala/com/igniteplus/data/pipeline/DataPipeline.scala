package com.igniteplus.data.pipeline
import com.igniteplus.data.pipeline.constants
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.PipelineService
import com.igniteplus.data.pipeline.transform.Cleanser
import org.apache.log4j.Logger
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.sql.{Column, DataFrame, internal}
import org.apache.spark.sql.functions.col

import scala.tools.nsc.MainBench.theCompiler.logError



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
