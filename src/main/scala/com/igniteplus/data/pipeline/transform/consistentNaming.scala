package com.igniteplus.data.pipeline.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

object consistentNaming {
 def consistentNaming(df:DataFrame):DataFrame = {
//    val consistentNames: DataFrame = df.withColumn("redirection_source", expr(
//      """
//        |case when redirection_source=="Google" || redirection_source=="GOOGLE" || redirection_source=="google" then redirection_source="Google"
//        |when redirection_source=="Facebook" || redirection_source=="FACEBOOK" || redirection_source=="facebook" then redirection_source="Facebook"
//        |when redirection_source=="LinkedIn" || redirection_source=="linkedin" || redirection_source=="LINKEDIN" then redirection_source="LinkedIn"
//        |when redirection_source=="Youtube" || redirection_source=="YOUTUBE" || redirection_source=="youtube" then redirection_source="YouTube"
//        |when redirection_source=="Pinterest" || redirection_source=="PINTEREST" || redirection_source=="pinterest" then redirection_source="Pinterest"
//        |end
//        |""".stripMargin))
    val consistentNames=df.withColumn("redirection_source",
      expr(" case when redirection_source=='Google' or redirection_source=='GOOGLE' or redirection_source=='google' then 'Google' " +
        "when redirection_source=='Facebook' || redirection_source=='FACEBOOK' || redirection_source=='facebook' then 'Facebook'"+
        "when redirection_source=='LinkedIn' or redirection_source=='linkedin' or redirection_source=='LINKEDIN' then 'LinkedIn'"+
        "when redirection_source=='Youtube' || redirection_source=='YOUTUBE' || redirection_source=='youtube' then 'YouTube'"+
        "when redirection_source=='Pinterest' or redirection_source=='PINTEREST' or redirection_source=='pinterest' then 'Pinterest' end"))
      consistentNames
  }
}
