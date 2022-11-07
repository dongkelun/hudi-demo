package com.dkl.hudi_spark2_4.hudi0_9

import com.dkl.utils.FsUtils.listDirs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool, MultiPartKeysValueExtractor}
import org.apache.spark.sql.SparkSession

import java.util

/**
 * Created by dongkelun on 2022/2/22 19:52
 *
 * spark-submit --master yarn --deploy-mode client --executor-memory 2G --num-executors 3 --executor-cores 2	\
 * --driver-memory 4G --driver-cores 2 --class com.dkl.hudi_spark2_4.hudi0_9.SyncHiveWithDatabase 	\
 * --principal hive/192-164-44-128.indata.com@INDATA.COM	\
 * --keytab /etc/security/keytabs/hive.service.keytab \
 * --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
 * hudi0.9_spark2.4-1.0.jar \
 * test_oracle_sync
 */
object SyncHiveWithDatabase {
  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: SyncHiveWithDatabase <hiveDatabaseName>")

    val spark = SparkSession.builder().appName("SyncHiveWithDatabase").
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension").
      config("spark.sql.parquet.writeLegacyFormat", true).
      enableHiveSupport().
      getOrCreate()
    val hiveDatabaseName = args(0)
    val databasePath = spark.catalog.getDatabase(hiveDatabaseName).locationUri
    var num = 1
    listDirs(databasePath).foreach(path => {
      val pathStr = path.toString
      val pathSplit = pathStr.split("/")
      val tableName = pathSplit(pathSplit.length - 1)
      val basePath = new Path(pathStr)
      val fs = basePath.getFileSystem(spark.sessionState.newHadoopConf())
      val hoodiePath = new Path(pathStr + "/" + HoodieTableMetaClient.METAFOLDER_NAME)
      if (fs.exists(hoodiePath)) {
        println(s"开始同步第${num}张表：$hiveDatabaseName.$tableName,表路径：$pathStr")
        val hiveConf: HiveConf = new HiveConf()
        hiveConf.addResource(fs.getConf)
        try {
          val tableMetaClient = HoodieTableMetaClient.builder.setConf(fs.getConf).setBasePath(pathStr).build
          val recordKeyFields = tableMetaClient.getTableConfig.getRecordKeyFields
          var keys = ""
          if (recordKeyFields.isPresent) {
            keys = recordKeyFields.get().mkString(",")
          }
          var partitionPathFields: util.List[String] = null
          val partitionFields = tableMetaClient.getTableConfig.getPartitionFields
          if (partitionFields.isPresent) {
            import scala.collection.JavaConverters._
            partitionPathFields = partitionFields.get().toList.asJava
          }
          val hiveSyncConfig = getHiveSyncConfig(pathStr, hiveDatabaseName, tableName, partitionPathFields, keys)
          new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable()
          println(s"同步第${num}张表：$hiveDatabaseName.$tableName 完成！！！")
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
            println(s"同步第${num}张表：$hiveDatabaseName.$tableName 失败！！！" + ex.getMessage)
          }
        }
      } else {
        println(s"第${num}张表：$hiveDatabaseName.$tableName 不是Hudi表，已跳过！！！")
      }
      num += 1
    })
    spark.stop()
  }

  def getHiveSyncConfig(basePath: String, dbName: String, tableName: String,
                        partitionPathFields: util.List[String] = null, keys: String = null): HiveSyncConfig = {
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig
    hiveSyncConfig.syncMode = HiveSyncMode.HMS.name
    hiveSyncConfig.createManagedTable = true
    hiveSyncConfig.databaseName = dbName
    hiveSyncConfig.tableName = tableName
    hiveSyncConfig.basePath = basePath
    hiveSyncConfig.partitionValueExtractorClass = classOf[MultiPartKeysValueExtractor].getName
    if (partitionPathFields != null && !partitionPathFields.isEmpty) hiveSyncConfig.partitionFields = partitionPathFields
    if (!StringUtils.isNullOrEmpty(keys)) hiveSyncConfig.serdeProperties = "primaryKey = " + keys //Spark SQL 更新表时需要该属性确认主键字段
    hiveSyncConfig
  }
}
