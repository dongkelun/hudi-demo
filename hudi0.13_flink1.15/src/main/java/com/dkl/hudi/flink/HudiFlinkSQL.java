package com.dkl.hudi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.configuration.FlinkOptions;

import java.util.concurrent.ExecutionException;

import static org.apache.hudi.examples.quickstart.utils.QuickstartConfigurations.sql;

public class HudiFlinkSQL {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        String tableName = "t1";
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
//                .option(FlinkOptions.TABLE_TYPE, "COPY_ON_WRITE")
                .partitionField("dt")
                .pkField("id")
                .end();
        tableEnv.executeSql(hoodieTableDDL);

        TableResult insertResult = tableEnv.executeSql(String.format("insert into %s values (1,'hudi',10,100,'2023-05-28')", tableName));
        try {
            insertResult.getJobClient().get().getJobExecutionResult().get();
        } catch (InterruptedException | ExecutionException ex) {
            // ignored
        }
        tableEnv.executeSql(String.format("select * from %s", tableName)).print();
    }
}
