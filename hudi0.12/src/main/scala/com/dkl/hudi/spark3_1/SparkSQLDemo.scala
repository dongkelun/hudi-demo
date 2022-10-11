package com.dkl.hudi.spark3_1

import org.apache.spark.sql.SparkSession

/**
 * Hudi Spark SQL Demo
 */
object SparkSQLDemo {
  val tableName = "test_hudi_table"

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

    testCreateTable(spark)

    testInsertTable(spark)
    testUpdateTable(spark)
    testDeleteTable(spark)
    testMergeTable(spark)

    testQueryTable(spark)

    spark.sql(s"drop table if exists $tableName")

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
    spark.sql(s"insert into $tableName values (1,'hudi',10,100,'2022-09-05'),(2,'hudi',10,100,'2022-09-05')")
    spark.sql(
      s"""insert into $tableName
         |select 3 as id, 'hudi' as name, 10 as price, 100 as ts, '2022-09-25' as dt union
         |select 4 as id, 'hudi' as name, 10 as price, 100 as ts, '2022-09-25' as dt
         |""".stripMargin)

  }

  def testQueryTable(spark: SparkSession): Unit = {
    spark.sql(s"select * from $tableName").show()
  }

  def testUpdateTable(spark: SparkSession): Unit = {
    spark.sql(s"update $tableName set price = 20.0 where id = 1")
  }

  def testDeleteTable(spark: SparkSession): Unit = {
    spark.sql(s"delete from $tableName where id = 1")
  }

  def testMergeTable(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |merge into $tableName as t0
         |using (
         |  select 1 as id, 'hudi' as name, 112 as price, 98 as ts, '2022-09-05' as dt,'INSERT' as opt_type union
         |  select 2 as id, 'hudi_2' as name, 10 as price, 100 as ts, '2022-09-05' as dt,'UPDATE' as opt_type union
         |  select 3 as id, 'hudi' as name, 10 as price, 100 as ts, '2021-09-25' as dt ,'DELETE' as opt_type
         | ) as s0
         |on t0.id = s0.id
         |when matched and opt_type!='DELETE' then update set *
         |when matched and opt_type='DELETE' then delete
         |when not matched and opt_type!='DELETE' then insert *
         |""".stripMargin)
  }
}
