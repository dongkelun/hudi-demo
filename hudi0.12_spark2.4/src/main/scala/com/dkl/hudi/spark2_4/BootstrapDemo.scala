package com.dkl.hudi.spark2_4

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.hive.{HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BootstrapDemo {
  lazy val spark = SparkSession.builder().
    master("local[*]").
    appName("BootstrapDemo").
    config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    // 扩展Spark SQL，使Spark SQL支持Hudi
    //    config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension").
    getOrCreate()
  val tableName = "test_hudi_bootstrap"

  def main(args: Array[String]): Unit = {
    testMetadataBootstrapCowNonPartitioned
    testMetadataBootstrapCOWHiveStylePartitioned
    testMetadataBootstrapCOWHiveStylePartitioned2
    testMetadataBootstrapCOWPartitioned
    testFullBootstrapCowNonPartitioned
    //    testFullBootstrapCOWHiveStylePartitioned
    testFullBootstrapCOWHiveStylePartitioned2
    testFullBootstrapCOWPartitioned

    spark.stop()
  }

  def testMetadataBootstrapCowNonPartitioned(): Unit = {
    val srcPath = "/tmp/bootstrap/metadata/src"
    val basePath = "/tmp/bootstrap/metadata/base"
    import spark.implicits._
    val sourceDF = Seq((1, "a1", 10, 1000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    sourceDF.write.mode("overwrite").parquet(srcPath)

    runMetadataBootstrap(srcPath, basePath)

    spark.read.format("hudi").load(basePath).show(false)

    val dfAppend = Seq((2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath)
    spark.read.format("hudi").load(basePath).show(false)
  }

  def testMetadataBootstrapCOWHiveStylePartitioned(): Unit = {
    import spark.implicits._
    val srcPath = "/tmp/bootstrap/metadata/src_hiveStylePartition"
    val basePath = "/tmp/bootstrap/metadata/base_hiveStylePartition"
    val sourceDF = Seq((1, "a1", 10, 1000, "2022-10-08"), (2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt")
    sourceDF.write.mode("overwrite").partitionBy("dt").parquet(srcPath)
    val extraOpts = Map(
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true"
    )
    runMetadataBootstrap(srcPath, basePath, "dt", extraOpts)

    spark.read.format("hudi").load(basePath).show(false)
    val dfAppend = Seq((2, "a2", 22, 2200, "2022-10-09"), (4, "a4", 40, 4000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath, "dt", extraOpts)
    spark.read.format("hudi").load(basePath).show(false)
  }

  def testMetadataBootstrapCOWHiveStylePartitioned2(): Unit = {
    import spark.implicits._
    val srcPath = "/tmp/bootstrap/metadata/src_hiveStylePartition2"
    val basePath = "/tmp/bootstrap/metadata/base_hiveStylePartition2"
    val sourceDF = Seq((1, "a1", 10, 1000, "2022-10-08"), (2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt")
    val partitions = Seq("2022-10-08", "2022-10-09")
    partitions.foreach(partition => {
      sourceDF
        .filter(sourceDF("dt").equalTo(lit(partition)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/dt=" + partition)
    })
    val extraOpts = Map(
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true"
    )
    runMetadataBootstrap(srcPath, basePath, "dt", extraOpts)

    spark.read.format("hudi").load(basePath).show(false)
    val dfAppend = Seq((2, "a2", 22, 2200, "2022-10-09"), (4, "a4", 40, 4000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath, "dt", extraOpts)
    spark.read.format("hudi").load(basePath).show(false)
  }

  def testMetadataBootstrapCOWPartitioned(): Unit = {
    import spark.implicits._
    val srcPath = "/tmp/bootstrap/metadata/src_partition"
    val basePath = "/tmp/bootstrap/metadata/base_partition"
    val sourceDF = Seq((1, "a1", 10, 1000, "2022-10-08"), (2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt")
    val partitions = Seq("2022-10-08", "2022-10-09")
    partitions.foreach(partition => {
      sourceDF
        .filter(sourceDF("dt").equalTo(lit(partition)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partition)
    })
    runMetadataBootstrap(srcPath, basePath, "dt")

    spark.read.format("hudi").load(basePath).show(false)
    val dfAppend = Seq((2, "a2", 22, 2200, "2022-10-09"), (4, "a4", 40, 4000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath, "dt")
    spark.read.format("hudi").load(basePath).show(false)
  }


  def testFullBootstrapCowNonPartitioned(): Unit = {
    val srcPath = "/tmp/bootstrap/full/src"
    val basePath = "/tmp/bootstrap/full/base"
    import spark.implicits._
    val sourceDF = makeDfColNullable(Seq((1, "a1", 10, 1000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt"))
    //    sourceDF.printSchema()

    sourceDF.write.mode("overwrite").parquet(srcPath)
    runFullBootstrap(srcPath, basePath)

    spark.read.format("hudi").load(basePath + "/*").show(false)

    val dfAppend = Seq((2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath)
    spark.read.format("hudi").load(basePath).show(false)
  }

  /**
   * hive分区格式的parquet文件因没有分区字段值，FullBootstrap暂时（0.12.0版本）不支持
   */
  def testFullBootstrapCOWHiveStylePartitioned(): Unit = {
    import spark.implicits._
    val srcPath = "/tmp/bootstrap/full/src_hiveStylePartition"
    val basePath = "/tmp/bootstrap/full/base_hiveStylePartition"
    val sourceDF = makeDfColNullable(Seq((1, "a1", 10, 1000, "2022-10-08"),
      (2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt"))
    sourceDF.write.mode("overwrite").partitionBy("dt").parquet(srcPath)
    val extraOpts = Map(
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true"
      //      DROP_PARTITION_COLUMNS.key() -> "true"
    )
    runFullBootstrap(srcPath, basePath, "dt", extraOpts)

    spark.read.format("hudi").load(basePath).show(false)
    val dfAppend = Seq((2, "a2", 22, 2200, "2022-10-09"), (4, "a4", 40, 4000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath, "dt", extraOpts)
    spark.read.format("hudi").load(basePath).show(false)
  }

  def testFullBootstrapCOWHiveStylePartitioned2(): Unit = {
    import spark.implicits._
    val srcPath = "/tmp/bootstrap/full/src_hiveStylePartition2"
    val basePath = "/tmp/bootstrap/full/base_hiveStylePartition2"
    val sourceDF = makeDfColNullable(Seq((1, "a1", 10, 1000, "2022-10-08"),
      (2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt"))
    val partitions = Seq("2022-10-08", "2022-10-09")
    partitions.foreach(partition => {
      sourceDF
        .filter(sourceDF("dt").equalTo(lit(partition)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/dt=" + partition)
    })
    val extraOpts = Map(
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true"
    )
    runFullBootstrap(srcPath, basePath, "dt", extraOpts)

    spark.read.format("hudi").load(basePath).show(false)
    val dfAppend = Seq((2, "a2", 22, 2200, "2022-10-09"), (4, "a4", 40, 4000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath, "dt", extraOpts)
    spark.read.format("hudi").load(basePath).show(false)
  }

  def testFullBootstrapCOWPartitioned(): Unit = {
    import spark.implicits._
    val srcPath = "/tmp/bootstrap/full/src_partition"
    val basePath = "/tmp/bootstrap/full/base_partition"
    val sourceDF = makeDfColNullable(Seq((1, "a1", 10, 1000, "2022-10-08"),
      (2, "a2", 20, 2000, "2022-10-09")).toDF("id", "name", "value", "ts", "dt"))
    val partitions = Seq("2022-10-08", "2022-10-09")
    partitions.foreach(partition => {
      sourceDF
        .filter(sourceDF("dt").equalTo(lit(partition)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partition)
    })
    runFullBootstrap(srcPath, basePath, "dt")

    spark.read.format("hudi").load(basePath).show(false)
    val dfAppend = Seq((2, "a2", 22, 2200, "2022-10-09"), (4, "a4", 40, 4000, "2022-10-08")).toDF("id", "name", "value", "ts", "dt")
    append2Hudi(dfAppend, basePath, "dt")
    spark.read.format("hudi").load(basePath).show(false)
  }

  def runMetadataBootstrap(srcPath: String, basePath: String, partitionField: String = "", extraOpts: Map[String, String] = Map.empty): Unit = {
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write.
      format("hudi").
      options(extraOpts).
      option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL).
      option(HoodieWriteConfig.TBL_NAME.key, tableName).
      option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id").
      option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionField).
      option(HoodieBootstrapConfig.BASE_PATH.key, srcPath).
      option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName).
      option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName).
      // 同步Hive相关的配置
      //      option(HoodieSyncConfig.META_SYNC_ENABLED.key(), true).
      //      option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, true).
      //      option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, HiveSyncMode.HMS.name()).
      //      option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key, "test").
      //      option(HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE.key(), true).
      //      option(HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE.key, true).
      //      option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key, tableName).
      //      option(HIVE_STYLE_PARTITIONING.key, true).
      //      option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key, partitionField).
      //      option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key, classOf[MultiPartKeysValueExtractor].getName).
      mode(SaveMode.Overwrite).
      save(basePath)
  }

  def runFullBootstrap(srcPath: String, basePath: String, partitionField: String = "", extraOpts: Map[String, String] = Map.empty): Unit = {
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write.format("hudi").
      options(extraOpts).
      option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL).
      option(HoodieWriteConfig.TBL_NAME.key, tableName).
      option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id").
      option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionField).
      option(HoodieBootstrapConfig.BASE_PATH.key, srcPath).
      option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName).
      option(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key, classOf[FullRecordBootstrapModeSelector].getName).
      option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName).
      // 同步Hive相关的配置
      //      option(HoodieSyncConfig.META_SYNC_ENABLED.key(), true).
      //      option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, true).
      //      option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, HiveSyncMode.HMS.name()).
      //      option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key, "test").
      //      option(HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE.key(), true).
      //      option(HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE.key, true).
      //      option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key, tableName).
      //      option(HIVE_STYLE_PARTITIONING.key, true).
      //      option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key, partitionField).
      //      option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key, classOf[MultiPartKeysValueExtractor].getName).
      mode(SaveMode.Overwrite)
      .save(basePath)
  }

  /**
   * upsert Hudi,验证insert和update hudi bootstrap表
   */
  def append2Hudi(dfAppend: DataFrame, basePath: String, partitionField: String = "", extraOpts: Map[String, String] = Map.empty): Unit = {
    dfAppend.write.format("hudi").
      options(extraOpts).
      option(HoodieWriteConfig.TBL_NAME.key, tableName).
      option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id").
      option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionField).
      option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName).
      mode(SaveMode.Append).
      save(basePath)
  }

  def makeDfColNullable(df: DataFrame): DataFrame = {
    val schema = df.schema.map(s => {
      StructField(s.name, s.dataType, true, s.metadata)
    })
    spark.createDataFrame(df.rdd, StructType(schema))
  }

}
