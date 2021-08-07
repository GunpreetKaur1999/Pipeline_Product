package com.igniteplus.data.pipeline.transform

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, trim}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile

object Cleanser {

  /*FUNCTION TO REMOVE DUPLICATES*/
  def deDuplication(df:DataFrame,toOrderBy:String,filterExp:String,refColumn:String,colNames : String*): DataFrame = {
      val winSpec = Window.partitionBy(colNames.head, colNames.tail:_*)
          .orderBy(desc(toOrderBy))
        val duplicate: DataFrame = df.withColumn(refColumn, row_number().over(winSpec))
          .filter(filterExp)
          .drop(refColumn)
        duplicate
     }

//def deDuplication(df : DataFrame, orderBy : String, colNames : String*) : DataFrame =
//{
//if(orderBy == "nil")
//{
//val deDuplicate : DataFrame = df.dropDuplicates(colNames.head, colNames.tail:_*)
//deDuplicate
//}
//else
//{
//val winSpec = Window.partitionBy(colNames: _*)
//.orderBy(desc(orderBy))
//val deDuplicate : DataFrame = df.withColumn("row_number", row_number().over(winSpec))
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
  def nullValuesCheckAndRemove(df:DataFrame,columnName:Seq[String],fileType:String,filePath:String) = {
    var notNullDf:DataFrame=df
    for (columnName <- columnName) {
      notNullDf = df.filter(df(columnName).isNotNull)
    }
    notNullDf
  }

  /*WRITE NULL VALUES TO A FILE*/
  def writeNullValues(df:DataFrame,columnName:Seq[String],fileType:String,filePath:String) = {
    var nullDf:DataFrame=df
    for (columnName <- columnName) {
      nullDf = df.filter(df(columnName).isNull)
    }
    writeFile(nullDf,fileType,filePath)
  }

}
