package com.dkl.hudi.java;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.*;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfigHolder;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry;
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES;

/**
 * Hudi Java Client 代码示例 (基于Hudi 0.12.0)
 * 实现读取Hive表转化为Hudi表,同步Hive元数据实现自动建表
 * 支持insert/upsert/delete
 * 可自行模拟读取mysql增量数据同步Hudi表
 */
public class JavaClientHive2Hudi {
    private static final Logger LOG = LogManager.getLogger(HoodieJavaWriteClientExample.class);

    private static final String TABLE_TYPE = HoodieTableType.COPY_ON_WRITE.name();

    private static final String INSERT_OPERATION = WriteOperationType.INSERT.value();
    private static final String UPSERT_OPERATION = WriteOperationType.UPSERT.value();
    private static final String DELETE_OPERATION = WriteOperationType.DELETE.value();

    protected static final String DEFAULT_PARTITION_PATH = "default";
    public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

    protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
    protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

    private static String HIVE_JDBC_URL = "jdbc:hive2://192.168.44.128:10000/test_hudi;principal=hive/indata-192-168-44-128.indata.com@INDATA.COM";

    private static String localKeytab = "/conf/hive.service.keytab";
    private static String principal = "hive/indata-192-168-44-128.indata.com@INDATA.COM";
    private static String krb5 = "/conf/krb5.conf";
    private static String coreSitePath = "/conf/core-site.xml";
    private static String hdfsSitePath = "/conf/hdfs-site.xml";

    private static String hiveSitePath = "/conf/hive-site.xml";
    private static String dbName = "test_hudi";
    //源表，hive历史表
    private static String sourceTable = "test_source";
    //目标表，hudi表
    private static String targetTable = "test_hudi_target";
    private static String tablePath = "/test_hudi/test_hudi_target";

    private static String recordKeyFields = "id";
    private static String orderingField = "ts";

    private static String preCombineField = "ts";
    private static String partitionFields = "dt";
    private static Long smallFileLimit = 25 * 1024 * 1024L;
    private static Long maxFileSize = 32 * 1024 * 1024L;

    private static Integer recordSizeEstimate = 64;

    private static String writeOperationType = INSERT_OPERATION;
    private static Connection conn = null;
    private static HoodieJavaWriteClient<HoodieRecordPayload> writeClient = null;
    private static FileSystem fs = null;


    /**
     * 创建Hive表模拟历史数据转化
     * create database test_hudi;
     * <p>
     * create table test_hudi.test_source (
     * id int,
     * name string,
     * price double,
     * dt string,
     * ts bigint
     * );
     * <p>
     * insert into test_hudi.test_source values (105,'hudi', 10.0,'2021-05-05',100);
     */

    public static void main(String[] args) {
        try {
            authenticate(localKeytab, principal, krb5);
            Map<String, String> options = new HashMap<>();
            options.put("url", HIVE_JDBC_URL);
            options.put("driver", "org.apache.hive.jdbc.HiveDriver");
            options.put("dbtable", dbName + "." + sourceTable);
            options.put("nullable", "true");
            conn = createConnectionFactory(options);
            // 读取Hive表的Schema作为写Hudi表的Schema
            // 也可以先建好Hudi目标表，直接读取Hudi表的Schema作为写Hudi表的Schema
            // 也可以像 #{@link HoodieJavaWriteClientExample}中一样直接提供一个Schema字符串
            Schema writeSchema = getJDBCSchema(options);

            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hadoopConf.set("fs.hdfs.impl.disable.cache", "true");
            hadoopConf.addResource(new Path(coreSitePath));
            hadoopConf.addResource(new Path(hdfsSitePath));
            fs = FSUtils.getFs(tablePath, hadoopConf);

            Path path = new Path(tablePath);
            Path hoodiePath = new Path(tablePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME);
            if (!fs.exists(path)) { //根据Hudi路径存不存在，判断Hudi表需不需要初始化
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(TABLE_TYPE)
                        .setTableName(targetTable)
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .initTable(hadoopConf, tablePath);
            }

            List<String> writeFiledNames = writeSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
            boolean shouldCombine = writeOperationType.equals(UPSERT_OPERATION) && writeFiledNames.contains(preCombineField);
            boolean shouldOrdering = writeOperationType.equals(UPSERT_OPERATION) && writeFiledNames.contains(orderingField);
            String payloadClassName = shouldOrdering ? DefaultHoodieRecordPayload.class.getName() :
                    shouldCombine ? OverwriteWithLatestAvroPayload.class.getName() : HoodieAvroPayload.class.getName();

            if (!(fs.exists(path) && fs.exists(hoodiePath))) { //根据Hudi路径存不存在，判断Hudi表需不需要初始化
                if (Arrays.asList(INSERT_OPERATION, UPSERT_OPERATION).contains(writeOperationType)) {
                    HoodieTableMetaClient.withPropertyBuilder()
                            .setTableType(TABLE_TYPE)
                            .setTableName(targetTable)
                            .setPayloadClassName(payloadClassName)
                            .setRecordKeyFields(recordKeyFields)
                            .setPreCombineField(preCombineField)
                            .setPartitionFields(partitionFields)
                            .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                            .initTable(hadoopConf, tablePath);
                } else if (writeOperationType.equals(DELETE_OPERATION)) { //Delete操作，Hudi表必须已经存在
                    throw new TableNotFoundException(tablePath);
                }
            }


            Properties indexProperties = new Properties();
            indexProperties.put(BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.key(), 150000); // 1000万总体时间提升1分钟
            HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                    .withSchema(writeSchema.toString())
                    .withParallelism(2, 2).withDeleteParallelism(2)
                    .forTable(targetTable)
                    .withWritePayLoad(payloadClassName)
                    .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(orderingField).build())
                    .withIndexConfig(HoodieIndexConfig.newBuilder()
                            .withIndexType(HoodieIndex.IndexType.BLOOM)
//                            .bloomIndexPruneByRanges(false) // 1000万总体时间提升1分钟
                            .bloomFilterFPP(0.000001)   // 1000万总体时间提升3分钟
                            .fromProperties(indexProperties)
                            .build())
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                            .compactionSmallFileSize(smallFileLimit)
                            .approxRecordSize(recordSizeEstimate).build())
                    .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(150, 200).build())
                    .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(100).build())
                    .withStorageConfig(HoodieStorageConfig.newBuilder()
                            .parquetMaxFileSize(maxFileSize).build())
                    .build();

            writeClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

            String newCommitTime = writeClient.startCommit();

            // 数据量大时，需要分批写，比如10万一个批次
            if (writeOperationType.equals(DELETE_OPERATION)) {
                writeClient.delete(getKeysForDelete(selectTable(options), writeSchema), newCommitTime);
            } else {
                List<HoodieRecord<HoodieRecordPayload>> records =
                        generateRecord(selectTable(options), writeSchema, payloadClassName, shouldCombine);

                if (writeOperationType.equals(UPSERT_OPERATION)) {
                    writeClient.upsert(records, newCommitTime);
                } else {
                    writeClient.insert(records, newCommitTime);
                }
            }

            //同步Hive元数据
            HiveConf hiveConf = getHiveConf(hiveSitePath, coreSitePath, hdfsSitePath);
            syncHive(getHiveSyncProperties(tablePath), hiveConf);

        } catch (Exception e) {
            LOG.error(e);
        } finally {
            close(conn);
            cleanupClients();
        }
    }

    /**
     * 利用HiveSyncTool同步Hive元数据
     * Spark写Hudi同步hive元数据的源码就是这样同步的
     *
     * @param properties
     * @param hiveConf
     */
    public static void syncHive(TypedProperties properties, HiveConf hiveConf) {
        HiveSyncTool hiveSyncTool = new HiveSyncTool(properties, hiveConf);
        hiveSyncTool.syncHoodieTable();
    }

    public static HiveConf getHiveConf(String hiveSitePath, String coreSitePath, String hdfsSitePath) {
        HiveConf configuration = new HiveConf();
        configuration.addResource(new Path(hiveSitePath));
        configuration.addResource(new Path(coreSitePath));
        configuration.addResource(new Path(hdfsSitePath));

        return configuration;
    }

    /**
     * 同步Hive元数据的一些属性配置
     * @param basePath
     * @return
     */
    public static TypedProperties getHiveSyncProperties(String basePath) {
        TypedProperties properties = new TypedProperties();
        properties.put(HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), HiveSyncMode.HMS.name());
        properties.put(HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE.key(), true);
        properties.put(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), dbName);
        properties.put(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), targetTable);
        properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), basePath);
        properties.put(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), MultiPartKeysValueExtractor.class.getName());
        properties.put(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), partitionFields);
        if (partitionFields != null && !partitionFields.isEmpty()) {
            properties.put(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), partitionFields);
        }

        return properties;
    }

    /**
     * 关闭client
     */
    protected static void cleanupClients() {
        closeWriteClient();
        try {
            cleanupFileSystem();
        } catch (IOException e) {
            LOG.error("cleanup hoodie " + dbName + "." + targetTable + "fileSystem error, ", e);
        }
    }

    protected static void closeWriteClient() {
        if (writeClient != null) {
            writeClient.close();
            writeClient = null;
        }
    }

    protected static void cleanupFileSystem() throws IOException {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    /**
     * 从ResultSet里获取内容，构造insert/upsert所需要的数据结构
     *
     * @param rs
     * @param writeSchema
     * @param payloadClassName
     * @param shouldCombine
     * @return List<HoodieRecord < HoodieRecordPayload>
     * @throws IOException
     * @throws SQLException
     */
    public static List<HoodieRecord<HoodieRecordPayload>> generateRecord(ResultSet rs,
                                                                         Schema writeSchema,
                                                                         String payloadClassName,
                                                                         boolean shouldCombine) throws IOException, SQLException {
        List<HoodieRecord<HoodieRecordPayload>> list = new ArrayList<>();

        while (rs.next()) {
            GenericRecord rec = new GenericData.Record(writeSchema);

            writeSchema.getFields().forEach(field -> {
                try {
                    rec.put(field.name(), convertValueType(rs, field.name(), field.schema().getType()));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            String partitionPath = partitionFields == null ? "" : getRecordPartitionPath(rs, writeSchema);
            System.out.println(partitionPath);
            String rowKey = recordKeyFields == null && writeOperationType.equals(INSERT_OPERATION) ? UUID.randomUUID().toString() : getRecordKey(rs, writeSchema);
            HoodieKey key = new HoodieKey(rowKey, partitionPath);
            if (shouldCombine) {
                Object orderingVal = HoodieAvroUtils.getNestedFieldVal(rec, preCombineField, false, false);
                list.add(new HoodieAvroRecord<>(key, createPayload(payloadClassName, rec, (Comparable) orderingVal)));
            } else {
                list.add(new HoodieAvroRecord<>(key, createPayload(payloadClassName, rec)));
            }

        }
        return list;
    }

    /**
     * 获取Delete需要的数据，仅需提供主键字段和分区字段构造HoodieKey即可
     *
     * @param rs
     * @param writeSchema
     * @return
     * @throws SQLException
     */
    private static List<HoodieKey> getKeysForDelete(ResultSet rs, Schema writeSchema) throws SQLException {
        List keysForDelete = new ArrayList();

        while (rs.next()) {
            String partitionPath = partitionFields == null ? "" : getRecordPartitionPath(rs, writeSchema);
            String rowKey = getRecordKey(rs, writeSchema);
            HoodieKey key = new HoodieKey(rowKey, partitionPath);
            keysForDelete.add(key);
        }
        return keysForDelete;
    }

    /**
     * Create a payload class via reflection, do not ordering/preCombine value.
     */
    public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record)
            throws IOException {
        try {
            return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
                    new Class<?>[]{Option.class}, Option.of(record));
        } catch (Throwable e) {
            throw new IOException("Could not create payload for class: " + payloadClass, e);
        }
    }

    /**
     * Create a payload class via reflection, passing in an ordering/preCombine value.
     */
    public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
            throws IOException {
        try {
            return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
                    new Class<?>[]{GenericRecord.class, Comparable.class}, record, orderingVal);
        } catch (Throwable e) {
            throw new IOException("Could not create payload for class: " + payloadClass, e);
        }
    }

    /**
     * 从ResultSet中获取主键字段对应的值
     *
     * @param rs
     * @param writeSchema
     * @return
     * @throws SQLException
     */
    private static String getRecordKey(ResultSet rs, Schema writeSchema) throws SQLException {
        boolean keyIsNullEmpty = true;
        StringBuilder recordKey = new StringBuilder();
        for (String recordKeyField : recordKeyFields.split(",")) {
            String recordKeyValue = getNestedFieldValAsString(rs, writeSchema, recordKeyField);
            recordKeyField = recordKeyField.toLowerCase();
            if (recordKeyValue == null) {
                recordKey.append(recordKeyField + ":" + NULL_RECORDKEY_PLACEHOLDER + ",");
            } else if (recordKeyValue.isEmpty()) {
                recordKey.append(recordKeyField + ":" + EMPTY_RECORDKEY_PLACEHOLDER + ",");
            } else {
                recordKey.append(recordKeyField + ":" + recordKeyValue + ",");
                keyIsNullEmpty = false;
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        if (keyIsNullEmpty) {
            throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
                    + recordKeyFields.toString() + " cannot be entirely null or empty.");
        }
        return recordKey.toString();
    }

    /**
     * 从ResultSet中获取主键字段对应的值
     *
     * @param rs
     * @param writeSchema
     * @return
     * @throws SQLException
     */
    private static String getRecordPartitionPath(ResultSet rs, Schema writeSchema) throws SQLException {
        if (partitionFields.isEmpty()) {
            return "";
        }

        StringBuilder partitionPath = new StringBuilder();
        String[] avroPartitionPathFields = partitionFields.split(",");
        for (String partitionPathField : avroPartitionPathFields) {
            String fieldVal = getNestedFieldValAsString(rs, writeSchema, partitionPathField);
            if (fieldVal == null || fieldVal.isEmpty()) {
                partitionPath.append(partitionPathField + "=" + DEFAULT_PARTITION_PATH);
            } else {
                partitionPath.append(partitionPathField + "=" + fieldVal);
            }
            partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
        }
        partitionPath.deleteCharAt(partitionPath.length() - 1);
        return partitionPath.toString();
    }

    /**
     * 根据字段名，从ResultSet获取对应的值
     *
     * @param rs
     * @param writeSchema
     * @param fieldName
     * @return
     * @throws SQLException
     */
    private static String getNestedFieldValAsString(ResultSet rs, Schema writeSchema, String fieldName) throws SQLException {
        Object value = null;
        if (writeSchema.getFields().stream().map(field -> field.name()).collect(Collectors.toList()).contains(fieldName)) {
            value = convertValueType(rs, fieldName, writeSchema.getField(fieldName).schema().getType());
        }
        return StringUtils.objToString(value);
    }

    /**
     * 根据字段名和字段数据类型，反正ResultSet中对应的字段的正确数据类型的值
     *
     * @param rs
     * @param name
     * @param dataType
     * @return
     * @throws SQLException
     */
    protected static Object convertValueType(ResultSet rs, String name, Schema.Type dataType) throws SQLException {
        Object value = null;
        if (dataType != null) {
            switch (dataType.toString().toLowerCase()) {
                case "int":
                case "smallint":
                case "tinyint":
                    value = rs.getInt(name);
                    break;
                case "bigint":
                case "long":
                    value = rs.getLong(name);
                    break;
                case "float":
                    value = rs.getFloat(name);
                    break;
                case "double":
                    value = rs.getDouble(name);
                    break;
                case "string":
                    value = rs.getString(name);
                    break;
                default:
                    if (rs.getObject(name) == null)
                        value = null;
                    else
                        value = rs.getObject(name);
            }
        }
        return value;
    }

    /**
     * kerberos 认证
     *
     * @param localKeytab
     * @param principal
     * @param krb5Str
     * @throws IOException
     */
    public static void authenticate(String localKeytab, String principal, String krb5Str) throws IOException {
        System.clearProperty("java.security.krb5.conf");
        System.setProperty("java.security.krb5.conf", krb5Str);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, localKeytab);
        LOG.info("Safety certification passed!");
    }

    /**
     * 获取Hive表的Schema
     *
     * @param options
     * @return
     * @throws Exception
     */
    public static Schema getJDBCSchema(Map<String, String> options) throws Exception {
        String url = options.get(JDBCOptions.JDBC_URL());
        String table = options.get(JDBCOptions.JDBC_TABLE_NAME());
        JdbcDialect dialect = JdbcDialects.get(url);

        try (final Statement confStatement = conn.createStatement();
             PreparedStatement schemaQueryStatement = conn.prepareStatement(dialect.getSchemaQuery(table))) {
            try {
                // 执行set配置，仅对当前连接生效
                confStatement.execute("set hive.resultset.use.unique.column.names = false");
            } catch (SQLException e) {
                LOG.error("getJDBCSchema set config error, ", e);
            }

            try (ResultSet rs = schemaQueryStatement.executeQuery()) {
                StructType structType;
                if (Boolean.parseBoolean(options.get("nullable"))) {
                    structType = JdbcUtils.getSchema(rs, dialect, true);
                } else {
                    structType = JdbcUtils.getSchema(rs, dialect, false);
                }
                return convertStructTypeToAvroSchema(structType);
//                    return AvroConversionUtils.convertStructTypeToAvroSchema(structType, table, "hoodie." + table);
            }
        }
    }


    /**
     * 从表中获取数据
     *
     * @param options
     * @return
     * @throws SQLException
     */
    public static ResultSet selectTable(Map<String, String> options) throws SQLException {
        String table = options.get(JDBCOptions.JDBC_TABLE_NAME());
        String sql = String.format("select * from %s", table);
        return conn.createStatement().executeQuery(sql);
    }

    public static Schema convertStructTypeToAvroSchema(StructType structType) {
        String structName = "hoodie_" + targetTable + "_record";
        String recordNamespace = "hoodie." + targetTable;
        return SchemaConverters.toAvroType(structType, false, structName, recordNamespace);
    }

    /**
     * Returns a factory for creating connections to the given JDBC URL.
     *
     * @param options - JDBC options that contains url, table and other information.
     * @return
     * @throws SQLException if the driver could not open a JDBC connection.
     */
    private static Connection createConnectionFactory(Map<String, String> options) throws SQLException {
        String driverClass = options.get(JDBCOptions.JDBC_DRIVER_CLASS());
        DriverRegistry.register(driverClass);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        Driver driver = null;
        while (drivers.hasMoreElements()) {
            Driver d = drivers.nextElement();
            if (d instanceof DriverWrapper) {
                if (((DriverWrapper) d).wrapped().getClass().getCanonicalName().equals(driverClass)) {
                    driver = d;
                }
            } else if (d.getClass().getCanonicalName().equals(driverClass)) {
                driver = d;
            }
            if (driver != null) {
                break;
            }
        }

        Objects.requireNonNull(driver, String.format("Did not find registered driver with class %s", driverClass));

        Properties properties = new Properties();
        properties.putAll(options);
        Connection connect;
        String url = options.get(JDBCOptions.JDBC_URL());
        connect = driver.connect(url, properties);
        Objects.requireNonNull(connect, String.format("The driver could not open a JDBC connection. Check the URL: %s", url));
        return connect;
    }

    /**
     * 关闭连接
     *
     * @param connect
     */
    public static void close(Connection connect) {
        try {
            if (connect != null) {
                connect.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

}
