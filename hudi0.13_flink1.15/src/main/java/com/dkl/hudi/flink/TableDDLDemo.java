package com.dkl.hudi.flink;

import org.apache.hudi.configuration.FlinkOptions;

import static org.apache.hudi.examples.quickstart.utils.QuickstartConfigurations.sql;

public class TableDDLDemo {
    public static void main(String[] args) {
        String tableName = "t1";
        String tablePath = "/tmp/flink/hudi/" + tableName;
        String hoodieTableDDL = sql(tableName)
                .option(FlinkOptions.PATH, tablePath)
                .option(FlinkOptions.READ_AS_STREAMING, true)
                .option(FlinkOptions.TABLE_TYPE, "COPY_ON_WRITE")
                .end();
        System.out.println(hoodieTableDDL);
        System.out.println();
        hoodieTableDDL = sql(tableName)
                .field("id int")
                .field("name string")
                .field("price double")
                .field("ts bigint")
                .field("dt string")
                .option(FlinkOptions.PATH, tablePath)
                .noPartition()
                .pkField("id")
                .end();
        System.out.println(hoodieTableDDL);
    }
}
