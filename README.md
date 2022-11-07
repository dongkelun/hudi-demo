# hudi-demo
Apache Hudi 代码示例.

## 打包命令
因为hudi0.9_spark2.4依赖common，所以单独打这个模块会报错，在根本目录下打包所有模块又比较慢，可以这样打包
```bash
 mvn clean package -pl :common,hudi0.9_spark2.4
```

## 相关文章
* [Apache Hudi 入门学习总结](https://mp.weixin.qq.com/s/5raMUByVGDyVYMeAdW-LRw)
* [Hudi Spark SQL总结](https://mp.weixin.qq.com/s/hII86LSXPankmLarIZu9rg)
* [利用Hudi Bootstrap转化现有Hive表的parquet/orc文件为Hudi表](https://mp.weixin.qq.com/s/p_qKMSt7WJigu2vd4qlYlQ)
* [Hudi Java Client总结|读取Hive写Hudi代码示例](https://mp.weixin.qq.com/s/5raMUByVGDyVYMeAdW-LRw)

### 源码分析
