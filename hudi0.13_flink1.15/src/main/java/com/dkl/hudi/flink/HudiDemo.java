package com.dkl.hudi.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class HudiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String targetTable = "t1";
        if (args.length > 0) {
            targetTable = args[0];
        }
        String basePath = "/tmp/flink/hudi/" + targetTable;

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
//        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
//        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
//        options.put(FlinkOptions.HIVE_SYNC_ENABLED.key(), "true");
        options.put("hive_sync.mode", "hms");
        options.put("hive_sync.conf.dir", "/usr/hdp/3.1.0.0-78/hive/conf");
        options.put("hive_sync.db", "hudi");
        options.put("hive_sync.table", targetTable);
        options.put("hive_sync.partition_fields", "dt");
        options.put("hive_sync.partition_extractor_class", "org.apache.hudi.hive.HiveStylePartitionValueExtractor");
        options.put("hoodie.datasource.write.hive_style_partitioning", "true");
        options.put("hoodie.datasource.hive_sync.create_managed_table", "true");

//        options.put(FlinkOptions.READ_AS_STREAMING.key(), "true"); // this option enable the streaming read
//        options.put(FlinkOptions.READ_START_COMMIT.key(), "'20210316134557'"); // specifies the start commit instant time

        DataStream<RowData> dataStream = env.fromElements(
                GenericRowData.of(1, StringData.fromString("hudi1"), 1.1, 1000L, StringData.fromString("2023-04-07")),
                GenericRowData.of(2, StringData.fromString("hudi2"), 2.2, 2000L, StringData.fromString("2023-04-08"))
        );
//        dataStream.print();
        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("id int")
                .column("name string")
                .column("price double")
                .column("ts bigint")
                .column("dt string")
                .pk("id")
                .partition("dt")
                .options(options);

        builder.sink(dataStream, false); // The second parameter indicating whether the input data stream is bounded
        env.execute("Hudi_Api_Sink");
        DataStream<RowData> rowDataDataStream = builder.source(env);
        rowDataDataStream.print();
        env.execute("Hudi_Api_Source");
    }
}
