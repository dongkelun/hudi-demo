package com.dkl.hudi.spark3_1

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, END_INSTANTTIME, INCR_PATH_GLOB, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import com.dkl.utils.FsUtils._

import scala.reflect.io.File.separator

/**
 * Spark SQL增量查询Hudi表
 */
object IncrementalQuery {
  val tableName = "test_hudi_incremental"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local[*]").
      appName("SparkSQLDemo").
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      // 扩展Spark SQL，使Spark SQL支持Hudi
      config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension").
      // 支持Hive，本地测试时，注释掉
      //      enableHiveSupport().
      getOrCreate()

    // 重复运行，需要先将对应的Hudi表文件删掉
    deletePath(spark,s"${System.getProperty("user.dir")}${separator}spark-warehouse${separator}$tableName")

    import spark.implicits._

    testCreateTable(spark)
    testInsertTable(spark)

    spark.table(tableName).show()
    spark.sql(s"call show_commits(table => '$tableName')").select("commit_time").show()
    val commits = spark.sql(s"call show_commits(table => '$tableName')").select("commit_time").map(k => k.getString(0)).take(10)

    val beginTime = commits(commits.length - 2)
    val endTime = commits(1)
    println(beginTime)
    println(endTime)

    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    val basePath = table.storage.properties("path")

    // incrementally query data
    val incrementalDF = spark.read.format("hudi").
      option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key, beginTime).
//      option(END_INSTANTTIME.key, endTime).
//      option(INCR_PATH_GLOB.key, "/dt=2022-11*/*").
            load(basePath)
    // table(tableName)的形式，增量查询参数不生效
//      table(tableName)

    incrementalDF.createOrReplaceTempView(s"temp_$tableName")

    spark.sql(s"select * from  temp_$tableName").show()
    spark.stop()
  }

  def testCreateTable(spark: SparkSession): Unit = {

    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  dt string
         |) using hudi
         | partitioned by (dt)
         | options (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = 'cow'
         | )
         |""".stripMargin)
  }

  def testInsertTable(spark: SparkSession): Unit = {
    spark.sql(s"insert into $tableName values (1,'hudi',10,100,'2022-11-25')")
    spark.sql(s"insert into $tableName values (2,'hudi',10,100,'2022-11-25')")
    spark.sql(s"insert into $tableName values (3,'hudi',10,100,'2022-11-26')")
    spark.sql(s"insert into $tableName values (4,'hudi',10,100,'2022-12-26')")
    spark.sql(s"insert into $tableName values (5,'hudi',10,100,'2022-12-27')")
  }
}
