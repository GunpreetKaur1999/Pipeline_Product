package com.igniteplus.data.pipeline.transform

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, trim, when}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile

object Cleanser {

  /**
   * CHECKS FOR NULL VALUES IN PRIMARY KEY COLUMNS AND REMOVES THEM
   * @param inputDF to check and remove null values in
   * @param primaryKeyColumns of a particular given data
   * @return A dataframe with removed null values from primary key columns
   */

  def nullValueCheckAndRemove(inputDF: DataFrame, primaryKeyColumns :Seq[String]) : DataFrame = {
    val primaryKeyColumnsAsColumnDataType : Seq[Column] = primaryKeyColumns.map(x => col(x))
    val condition : Column = primaryKeyColumnsAsColumnDataType.map(x => x.isNull).reduce(_||_)
    val nullFlag : DataFrame = inputDF.withColumn("nullFlag",when(condition,"true").otherwise("false"))
    val notNullDF : DataFrame = nullFlag.filter("nullFlag==false")
    val notNullDf : DataFrame = notNullDF.drop("nullFlag")
    writeFile(notNullDf,"csv","output/merged-data/notNull.csv")
    notNullDf
  }















/*  /*FUNCTION TO REMOVE DUPLICATES*/
  def deDuplication(df:DataFrame,toOrderBy:String,filterExp:String,refColumn:String,colNames : String*): DataFrame = {
      val winSpec = Window.partitionBy(colNames.head, colNames.tail:_*)
          .orderBy(desc(toOrderBy))
        val duplicate: DataFrame = df.withColumn(refColumn, row_number().over(winSpec))
          .filter(filterExp)
          .drop(refColumn)
        duplicate
     }

//def deDuplication(inputDF : DataFrame, orderBy : String, colNames : String*) : DataFrame =
//{
//if(orderBy == "nil")
//{
//val deDuplicate : DataFrame = inputDF.dropDuplicates(colNames.head, colNames.tail:_*)
//deDuplicate
//}
//else
//{
//val winSpec = Window.partitionBy(colNames: _*)
//.orderBy(desc(orderBy))
//val deDuplicate : DataFrame = inputDF.withColumn("row_number", row_number().over(winSpec))
//.filter("row_number==1")
//.drop("row_number")
//deDuplicate
//}
//}

  /*FUCTION TO TRIM THE SPACES*/
  def trimFunction(df:DataFrame,columnToBeTrimmed:String):DataFrame = {
    val trimmed: DataFrame = df.withColumn(columnToBeTrimmed, trim(col(columnToBeTrimmed)))
    trimmed
  }

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


*/
}
