# hudi-demo
Apache Hudi 代码示例.

## 打包命令
因为hudi0.9_spark2.4依赖common，所以单独打这个模块会报错，在根本目录下打包所有模块又比较慢，可以这样打包
```bash
 mvn clean package -pl :common,hudi0.9_spark2.4
```

## 相关文章
* [Apache Hudi 入门学习总结](https://mp.weixin.qq.com/s/9cKherIGgeVxBeqY39nL2Q)
* [Hudi Spark SQL总结](https://mp.weixin.qq.com/s/hII86LSXPankmLarIZu9rg)
* [利用Hudi Bootstrap转化现有Hive表的parquet/orc文件为Hudi表](https://mp.weixin.qq.com/s/p_qKMSt7WJigu2vd4qlYlQ)
* [Hudi Java Client总结|读取Hive写Hudi代码示例](https://mp.weixin.qq.com/s/5raMUByVGDyVYMeAdW-LRw)
* [Flink Hudi DataStream API代码示例](https://mp.weixin.qq.com/s/83lMpbLCM3Mlb3fg_rEddw)

### 源码分析
* [Hudi源码|bootstrap源码分析总结（写Hudi）](https://mp.weixin.qq.com/s/9SOH3kOid0GSN31hF6M4dg)
* [Hudi preCombinedField 总结(二)-源码分析](https://mp.weixin.qq.com/s/vrBvB0SOCNPrMKHSUn4VOg)
* [Hudi Clean Policy 清理策略实现分析](https://mp.weixin.qq.com/s/YbVnkVyf7EbTAxNshX9gRA)
* [Hudi Clean 清理文件实现分析](https://mp.weixin.qq.com/s/97CpClzK1skczpszc8VeEg)
* [Hudi查询类型/视图总结](https://mp.weixin.qq.com/s/rdvCSHV5ObUSGMl9FciMfA)
* [Hudi Spark SQL源码学习总结-Create Table](https://mp.weixin.qq.com/s/8LiNiFe_kUS0oyN27PGTwg)
* [Hudi Spark SQL源码学习总结-CTAS](https://mp.weixin.qq.com/s/djuWfw0_abiifNnE7Usghg)
* [Hudi Spark源码学习总结-df.write.format("hudi").save](https://mp.weixin.qq.com/s/FlKoYL4ZqtYdzoxUOWx31w)
* [Hudi Spark源码学习总结-spark.read.format("hudi").load](https://mp.weixin.qq.com/s/FiJIyyondhZoofSWRTzADw)
* [Hudi Spark SQL源码学习总结-select（查询）](https://mp.weixin.qq.com/s/slscIdvbCB_BUPdWlFVt1Q)

