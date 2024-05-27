package com.dkl.hudi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;

import java.util.concurrent.ExecutionException;

import static com.dkl.hudi.flink.Configurations.sql;

public class TestMOR {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String tableName = "test_mor";
        if (args.length > 0) {
            tableName = args[0];
        }

        String tablePath = "/tmp/flink/hudi/" + tableName;
        String hoodieTableDDL = sql(tableName)
                .field("id int")
                .field("name string")
                .field("price double")
                .field("ts bigint")
                .field("dt string")
                .option(FlinkOptions.PATH, tablePath)
//                .option(FlinkOptions.READ_AS_STREAMING, true)
                .option(FlinkOptions.OPERATION, WriteOperationType.INSERT)
                .option(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ")
                .option("compaction.async.enabled", false)
                .option("compaction.delta_commits", 1)
//                .option("compaction.tasks", "4")
                .option("hoodie.compact.inline", true)
//                .option("hoodie.compact.schedule.inline", true) // 不要设置此参数
//                .option("hoodie.log.compaction.inline", true) // 不要设置此参数
                .option("hoodie.compact.inline.max.delta.commits", 1)
                .partitionField("dt")
                .pkField("id")
                .end();

        tableEnv.executeSql(hoodieTableDDL);
        TableResult insertResult = tableEnv.executeSql(String.format("insert into %s values (1,'hudi',10,100,'2024-05-22')", tableName));
        try {
            insertResult.getJobClient().get().getJobExecutionResult().get();
        } catch (InterruptedException | ExecutionException ex) {
            // ignored
        }
        tableEnv.executeSql(String.format("select * from %s", tableName)).print();
    }
}
