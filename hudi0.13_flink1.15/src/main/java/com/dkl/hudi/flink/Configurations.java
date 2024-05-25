package com.dkl.hudi.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.streamer.FlinkStreamerConfig;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Configurations for the test.
 */
public class Configurations {
    private Configurations() {
    }

    public static final DataType ROW_DATA_TYPE = DataTypes.ROW(
                    DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
                    DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
                    DataTypes.FIELD("age", DataTypes.INT()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
                    DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
            .notNull();

    public static final RowType ROW_TYPE = (RowType) ROW_DATA_TYPE.getLogicalType();


    private static final List<String> FIELDS = ROW_TYPE.getFields().stream()
            .map(RowType.RowField::asSummaryString).collect(Collectors.toList());

    public static final DataType ROW_DATA_TYPE_WIDER = DataTypes.ROW(
                    DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
                    DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
                    DataTypes.FIELD("age", DataTypes.INT()),
                    DataTypes.FIELD("salary", DataTypes.DOUBLE()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
                    DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
            .notNull();

    public static final RowType ROW_TYPE_WIDER = (RowType) ROW_DATA_TYPE_WIDER.getLogicalType();

    public static String getCreateHoodieTableDDL(String tableName, Map<String, String> options) {
        return getCreateHoodieTableDDL(tableName, options, true, "partition");
    }

    public static String getCreateHoodieTableDDL(
            String tableName,
            Map<String, String> options,
            boolean havePartition,
            String partitionField) {
        return getCreateHoodieTableDDL(tableName, FIELDS, options, havePartition, "uuid", partitionField);
    }

    public static String getCreateHoodieTableDDL(
            String tableName,
            List<String> fields,
            Map<String, String> options,
            boolean havePartition,
            String pkField,
            String partitionField) {
        StringBuilder builder = new StringBuilder();
        builder.append("create table ").append(tableName).append("(\n");
        for (String field : fields) {
            builder.append("  ").append(field).append(",\n");
        }
        builder.append("  PRIMARY KEY(").append(pkField).append(") NOT ENFORCED\n")
                .append(")\n");
        if (havePartition) {
            builder.append("PARTITIONED BY (`").append(partitionField).append("`)\n");
        }
        final String connector = options.computeIfAbsent("connector", k -> "hudi");
        builder.append("with (\n"
                + "  'connector' = '").append(connector).append("'");
        options.forEach((k, v) -> builder.append(",\n")
                .append("  '").append(k).append("' = '").append(v).append("'"));
        builder.append("\n)");
        return builder.toString();
    }

    public static String getCreateHudiCatalogDDL(final String catalogName, final String catalogPath) {
        StringBuilder builder = new StringBuilder();
        builder.append("create catalog ").append(catalogName).append(" with (\n");
        builder.append("  'type' = 'hudi',\n"
                + "  'catalog.path' = '").append(catalogPath).append("'");
        builder.append("\n)");
        return builder.toString();
    }



    public static String getCsvSourceDDL(String tableName, String fileName) {
        String sourcePath = Objects.requireNonNull(Thread.currentThread()
                .getContextClassLoader().getResource(fileName)).toString();
        return "create table " + tableName + "(\n"
                + "  uuid varchar(20),\n"
                + "  name varchar(10),\n"
                + "  age int,\n"
                + "  ts timestamp(3),\n"
                + "  `partition` varchar(20)\n"
                + ") with (\n"
                + "  'connector' = 'filesystem',\n"
                + "  'path' = '" + sourcePath + "',\n"
                + "  'format' = 'csv'\n"
                + ")";
    }

    public static Configuration getDefaultConf(String tablePath) {
        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.PATH, tablePath);
        conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH,
                Objects.requireNonNull(Thread.currentThread()
                        .getContextClassLoader().getResource("test_read_schema.avsc")).toString());
        conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
        conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");
        return conf;
    }

    public static FlinkStreamerConfig getDefaultStreamerConf(String tablePath) {
        FlinkStreamerConfig streamerConf = new FlinkStreamerConfig();
        streamerConf.targetBasePath = tablePath;
        streamerConf.sourceAvroSchemaPath = Objects.requireNonNull(Thread.currentThread()
                .getContextClassLoader().getResource("test_read_schema.avsc")).toString();
        streamerConf.targetTableName = "TestHoodieTable";
        streamerConf.partitionPathField = "partition";
        streamerConf.tableType = "COPY_ON_WRITE";
        streamerConf.checkpointInterval = 4000L;
        return streamerConf;
    }

    /**
     * Creates the tool to build hoodie table DDL.
     */
    public static Configurations.Sql sql(String tableName) {
        return new Configurations.Sql(tableName);
    }

    public static Configurations.Catalog catalog(String catalogName) {
        return new Configurations.Catalog(catalogName);
    }

    // -------------------------------------------------------------------------
    //  Utilities
    // -------------------------------------------------------------------------

    /**
     * Tool to build hoodie table DDL with schema .
     */
    public static class Sql {
        private final Map<String, String> options;
        private final String tableName;
        private List<String> fields = new ArrayList<>();
        private boolean withPartition = true;
        private String pkField = "uuid";
        private String partitionField = "partition";

        public Sql(String tableName) {
            options = new HashMap<>();
            this.tableName = tableName;
        }

        public Configurations.Sql option(ConfigOption<?> option, Object val) {
            this.options.put(option.key(), val.toString());
            return this;
        }

        public Configurations.Sql option(String key, Object val) {
            this.options.put(key, val.toString());
            return this;
        }

        public Configurations.Sql options(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        public Configurations.Sql noPartition() {
            this.withPartition = false;
            return this;
        }

        public Configurations.Sql pkField(String pkField) {
            this.pkField = pkField;
            return this;
        }

        public Configurations.Sql partitionField(String partitionField) {
            this.partitionField = partitionField;
            return this;
        }

        public Configurations.Sql field(String fieldSchema) {
            fields.add(fieldSchema);
            return this;
        }

        public String end() {
            if (this.fields.size() == 0) {
                this.fields = FIELDS;
            }
            return Configurations.getCreateHoodieTableDDL(this.tableName, this.fields, options,
                    this.withPartition, this.pkField, this.partitionField);
        }
    }

    public static class Catalog {
        private final String catalogName;
        private String catalogPath = ".";

        public Catalog(String catalogName) {
            this.catalogName = catalogName;
        }

        public Configurations.Catalog catalogPath(String catalogPath) {
            this.catalogPath = catalogPath;
            return this;
        }

        public String end() {
            return Configurations.getCreateHudiCatalogDDL(catalogName, catalogPath);
        }
    }
}
