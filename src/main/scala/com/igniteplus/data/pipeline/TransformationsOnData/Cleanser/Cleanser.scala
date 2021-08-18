package com.igniteplus.data.pipeline.TransformationsOnData.Cleanser

import com.igniteplus.data.pipeline.constants.ApplicationConstants.ROW_NUMBER
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, when}
import org.apache.spark.sql.{Column, DataFrame}

object Cleanser {

  /**
   * CHECKS FOR NULL VALUES IN PRIMARY KEY COLUMNS AND REMOVES THEM
   * @param inputDF           to check and remove null values in
   * @param primaryKeyColumns of a particular given data
   * @return A dataframe with removed null values from primary key columns
   */

  def nullValueCheckAndRemove(inputDF: DataFrame, primaryKeyColumns: Seq[String], writeOutputInFormat: String, writeOutputToPathNull: String, writeOutputToPathNotNull: String): DataFrame = {
    val primaryKeyColumnsAsColumnDataType: Seq[Column] = primaryKeyColumns.map(x => col(x))
    val condition: Column = primaryKeyColumnsAsColumnDataType.map(x => x.isNull).reduce(_ || _)
    val nullFlag: DataFrame = inputDF.withColumn("nullFlag", when(condition, "true").otherwise("false"))
    val notNullDF: DataFrame = nullFlag.filter("nullFlag==false")
    val nullDF: DataFrame = nullFlag.filter("nullFlag==true")
    val notNullDf: DataFrame = notNullDF.drop("nullFlag")
    writeFile(notNullDf, writeOutputInFormat, writeOutputToPathNotNull)
    writeFile(nullDF, writeOutputInFormat, writeOutputToPathNull)
    notNullDf
  }

  /**
   * TO REMOVE THE DUPLICATE DATA AND SELECT THE LASTEST WHERE IT WAS DUPLICATED
   * @param inputDF
   * @param filterExp
   * @param refColumn
   * @param colNames
   * @param toOrderBy
   * @param writeOutputInFormat
   * @param writeOutputToPath
   * @return a dataframe without duplicates and the latest value in case a deDuplicate was found
   */

  def deDuplication(inputDF: DataFrame, filterExp: String, refColumn: String, colNames: Seq[String], toOrderBy: Option[String], writeOutputInFormat: String, writeOutputToPath: String): DataFrame = {

    toOrderBy match {
      case Some(x) => {
        val winSpec = Window.partitionBy(colNames.head, colNames.tail: _*).orderBy(x)
        val deDuplicate = inputDF.withColumn(refColumn, row_number().over(winSpec))
          .filter(filterExp)
          .drop(refColumn)
        writeFile(deDuplicate, writeOutputInFormat, writeOutputToPath)
        deDuplicate
      }
      case _ => {
        val deDuplicate = inputDF.dropDuplicates(colNames.head, colNames.tail: _*)
        writeFile(deDuplicate, writeOutputInFormat, writeOutputToPath)
        deDuplicate
      }
    }
  }

  /**
   * FUNCTION TO REMOVE DUPLICATES
   * @param df the dataframe
   * @param primaryKeyColumns sequence of primary key columns of the df dataframe
   * @param orderByColumn
   * @return dataframe with no duplicates
   */
  def removeDuplicates(df: DataFrame,
                       primaryKeyColumns: Seq[String],
                       orderByColumn: Option[String]
                      ): DataFrame = {

    val dfDropDuplicates: DataFrame = orderByColumn match {
      case Some(orderCol) => {
        val windowSpec = Window.partitionBy(primaryKeyColumns.map(col): _*).orderBy(desc(orderCol))
        df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
          .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
      }
      case _ => df.dropDuplicates(primaryKeyColumns)
    }
    dfDropDuplicates
  }

//
//  /*FUCTION TO TRIM THE SPACES*/
//  def trimFunction(inputDF:DataFrame,columnToBeTrimmed:String):DataFrame = {
//    val trimmed: DataFrame = inputDF.withColumn(columnToBeTrimmed, trim(col(columnToBeTrimmed)))
//    trimmed
//  }

  /*FUNCTION TO CHECK NULL VALUES AND WRITE IT TO A FILE*/
//  def nullValuesCheckAndRemove(inputDF:DataFrame,columnName:Seq[String],fileType:String,filePath:String) = {
//    var notNullDf:DataFrame=inputDF
//    for (columnName <- columnName) {
//      notNullDf = inputDF.filter(inputDF(columnName).isNotNull)
//    }
//    notNullDf
//  }
//
//  /*WRITE NULL VALUES TO A FILE*/
//  def writeNullValues(inputDF:DataFrame,columnName:Seq[String],fileType:String,filePath:String) = {
//    var nullDf:DataFrame=inputDF
//    for (columnName <- columnName) {
//      nullDf = inputDF.filter(inputDF(columnName).isNull)
//    }
//    writeFile(nullDf,fileType,filePath)
//  }



}
