package com.dkl.utils

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

/**
 * Created by dongkelun on 2022/3/8 9:12
 */
object FsUtils {
  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**
   * 打印一级目录名
   */
  def listDirs(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory())
  }

  def deletePath(spark: SparkSession, pathString: String) = {
    val path = new Path(pathString)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }
}
