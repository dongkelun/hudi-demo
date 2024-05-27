package com.dkl.hudi.flink;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;

import static com.dkl.hudi.flink.Configurations.sql;

/**
 * 测试流写MOR表
 */
public class TestStreamingMOR {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(10000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql("CREATE TABLE random_source ( \n" +
                "  user_id INT,\n" +
                "  product VARCHAR,\n" +
                "  amount VARCHAR\n" +
                "  ) WITH ( \n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',           -- 每秒产生的数据条数\n" +
                "  'fields.user_id.kind' = 'sequence',   -- 有界序列（结束后自动停止输出）\n" +
                "  'fields.user_id.start' = '1',         -- 序列的起始值\n" +
                "  'fields.user_id.end' = '10000',       -- 序列的终止值\n" +
                "  'fields.product.length' = '5',\n" +
                "  'fields.amount.length' = '5'        -- 随机字符串的长度\n" +
                ")"
        );

        String tableName = "test_mor_orders";
        if (args.length > 0) {
            tableName = args[0];
        }

        String tablePath = "/tmp/flink/hudi/" + tableName;
        String hoodieTableDDL = sql(tableName)
                .field("user_id string")
                .field("product string")
                .field("amount string")
                .option(FlinkOptions.PATH, tablePath)
                .option(FlinkOptions.OPERATION, WriteOperationType.INSERT)
                .option(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ")
                .option("compaction.delta_commits", 2)
                .option("index.type", "BUCKET")
                .option("hoodie.bucket.index.num.buckets", 4)
                .noPartition()
                .pkField("user_id")
                .end();
        tableEnv.executeSql(hoodieTableDDL);

        tableEnv.executeSql(String.format("insert into %s select cast(user_id as String), product,amount from random_source", tableName));
    }
}
