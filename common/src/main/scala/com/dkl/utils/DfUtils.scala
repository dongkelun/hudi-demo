package com.dkl.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Created by dongkelun on 2022/2/15 15:10
 */
object DfUtils {

  /**
   * @param spark         SparkSession
   * @param database_conf 数据库配置项Map，包括driver,url,username,password,dbtable等内容，提交程序时需用--jars选项引用相关jar包
   * @return 返回DataFrame对象
   */
  def getDataFrame(spark: SparkSession, database_conf: scala.collection.Map[String, String]) = {
    spark.read.format("jdbc").options(database_conf).load()
  }

  def convertDf2LowerCase(df: DataFrame, spark: SparkSession): DataFrame = {
    val schemaWithLowercaseName = df.schema.map(s => {
      StructField(s.name.toLowerCase.replaceAll(" ", ""), s.dataType, s.nullable, s.metadata)
    })
    spark.createDataFrame(df.rdd, StructType(schemaWithLowercaseName))
  }

  def convertDf2String(df: DataFrame): DataFrame = {
    val colNames = df.columns
    val cols = colNames.map(f => col(f).cast(StringType))
    df.select(cols: _*)
  }
}
