package com.dkl.hudi_spark2_4.hudi0_9

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.dkl.utils.DfUtils.getDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * Created by dongkelun on 2022/3/11 15:55
 *
 * 跨集群同步Hive字段注释
 *
 * spark-submit --master yarn --deploy-mode client --executor-memory 2G --num-executors 10 --executor-cores 2	\
 * --driver-memory 4G --driver-cores 2 --class com.dkl.hudi_spark2_4.hudi0_9 	\
 * --principal hive/indata-192-164-44-128.indata.com@INDATA.COM	\
 * --keytab /etc/security/keytabs/hive.service.keytab \
 * --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
 * hudi0.9_spark2.4-1.0.jar \
 * test_hudi 'jdbc:mysql://192.168.44.138:3306/hive?useUnicode=true&characterEncoding=utf-8' hive hive123 \
 * test_hudi 'jdbc:mysql://indata-192-164-44-128.indata.com:3306/hive?useUnicode=true&characterEncoding=utf-8' hive hive123
 *
 */
object SyncCommentsAcrossClusters {
  def main(args: Array[String]): Unit = {
    require(args.length >= 5, "Usage: SyncCommentsAcrossClusters <old_hiveDatabaseName> " +
      "<old_mysqlUrl> <old_mysqlUser> <old_mysqlPassword> <new_hiveDatabaseName> " +
      "<new_mysqlUrl> <new_mysqlUser> <new_mysqlPassword>")

    val spark = SparkSession.builder().appName("SyncCommentsAcrossClusters").
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._

    val old_hiveDatabaseName = args(0)
    val old_mysqlUrl = args(1)
    val old_mysqlUser = args(2)
    val old_mysqlPassword = args(3)
    val new_hiveDatabaseName = args(4)
    require(spark.sql(s"show databases like '$new_hiveDatabaseName'").count() > 0,
      s"hive数据库：$new_hiveDatabaseName 不存在，请检查！！")


    //oracle的连接信息
    val p = new Properties()
    p.put("driver", "com.mysql.jdbc.Driver")
    p.put("url", old_mysqlUrl)
    p.put("user", old_mysqlUser)
    p.put("password", old_mysqlPassword)
    import scala.collection.JavaConversions._
    val database_conf: scala.collection.mutable.Map[String, String] = p
    database_conf.put("dbtable", getCommentSql(old_hiveDatabaseName))

    val hiveTables = spark.sql(s"show tables in $new_hiveDatabaseName")
    val hiveTableNames = hiveTables.select("tableName").distinct().collect().map(_.getAs[String]("tableName"))
    //所有的表字段对应的注释
    var allColComments = getDataFrame(spark, database_conf)
      .where("COMMENT is not null")
      .where(col("TBL_NAME").isin(hiveTableNames: _*))

    if (args.length == 8) {
      database_conf.put("url", args(5))
      database_conf.put("user", args(6))
      database_conf.put("password", args(7))
      database_conf.put("dbtable", getCommentSql(new_hiveDatabaseName))

      val newHiveComments = getDataFrame(spark, database_conf)
      val commentExistsTblColNames = newHiveComments.where("COMMENT is not null and COMMENT!=''")
        .select("TBL_COL_NAME").distinct().
        collect().map(_.getAs[String]("TBL_COL_NAME"))
      val existsColComments = allColComments.where(col("TBL_COL_NAME").isin(commentExistsTblColNames: _*))
      existsColComments.show()
      println(
        s"""已经有${existsColComments.select("TBL_NAME").distinct().count().intValue()}张表存在注释，
           |这些表不再重复添加注释：${existsColComments.select("TBL_NAME").distinct().collect().mkString(",")}
           |""".stripMargin)
      allColComments = allColComments.where(!col("TBL_COL_NAME").isin(commentExistsTblColNames: _*))

      allColComments = allColComments.drop("TYPE_NAME").
        join(newHiveComments.select("TBL_COL_NAME", "TYPE_NAME"), Seq("TBL_COL_NAME"))
      allColComments.show()
    }
    val commentHiveTableNames = allColComments.select("TBL_NAME").distinct().collect()
      .map(_.getAs[String]("TBL_NAME"))
    val tableCount = commentHiveTableNames.length
    val tableColCount = allColComments.select("TBL_COL_NAME").distinct().count().intValue()
    println(s"总共需要为${tableCount}张表,${tableColCount}个字段添加注释")
    var num = 1
    val comment_success_tableName = "temp_success_comment"
    val sync_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    commentHiveTableNames.foreach(hiveTableName => {
      try {
        println(s"---------------正在为第${num}张表：$hiveTableName 添加注释，总共${tableCount}张表--------------")
        allColComments.where(lower(col("TBL_NAME")).equalTo(hiveTableName)).collect().foreach(row => {
          val colName = row.getAs[String]("COLUMN_NAME").toLowerCase.replaceAll(" ", "")
          val comments = row.getAs[String]("COMMENT").replaceAll("'", "")
          val colType = row.getAs[String]("TYPE_NAME")
          println(s"$hiveTableName : $colName $comments $colType")
          if (!comments.equals("")) {
            val sql = s"alter table $new_hiveDatabaseName.$hiveTableName change $colName $colName $colType comment '$comments'"
            println(sql)
            try {
              spark.sql(sql)
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
                println(s"---------------第$num 张表：$hiveTableName 添加注释失败，请检查！！总共${tableCount}张表--------------")
            }
          }
        })
        Seq((num, hiveTableName, "success", sync_time)).toDF("num", "table_name", "mark", "sync_time").
          write.mode("append").
          saveAsTable(s"$new_hiveDatabaseName.$comment_success_tableName")
        println(s"---------------第$num 张表：$hiveTableName 添加注释完成！总共${tableCount}张表--------------")
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          println(s"---------------第$num 张表：$hiveTableName 添加注释失败，请检查！！总共${tableCount}张表--------------")
      }
      num += 1
    })

    spark.stop()
  }

  def getCommentSql(hiveDatabase: String): String = {
    s"""
       |(
       |SELECT DBS.DB_ID,DBS.NAME,TBL_ID,TBL_NAME,COLUMNS_V2.CD_ID,
       |COLUMNS_V2.COLUMN_NAME,COLUMNS_V2.TYPE_NAME,COLUMNS_V2.COMMENT,
       |CONCAT(TBL_NAME,'_',COLUMNS_V2.COLUMN_NAME) as TBL_COL_NAME
       |from TBLS
       |LEFT join DBS on TBLS.DB_ID=DBS.DB_ID
       |left join SDS on TBLS.SD_ID=SDS.SD_ID
       |LEFT join COLUMNS_V2 on SDS.CD_ID=COLUMNS_V2.CD_ID
       |where DBS.NAME='$hiveDatabase'
       |)a
       |""".stripMargin
  }

}
