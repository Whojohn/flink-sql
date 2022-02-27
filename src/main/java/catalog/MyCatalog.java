package catalog;

import catalog.pojo.PojoCatalogDatabase;
import catalog.pojo.PojoCatalogTableConnInfo;
import catalog.pojo.PojoCatalogTableSchema;
import lombok.SneakyThrows;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MyCatalog extends AbstractCatalog {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    public static final String MYSQL_CLASS = "com.mysql.cj.jdbc.Driver";
    protected final String username;
    protected final String pwd;
    protected final String defaultUrl;

    // do as memory catalog do
    private final Map<ObjectPath, CatalogBaseTable> tables;
    private final Map<String, CatalogDatabase> databases;
    private final Map<ObjectPath, Boolean> catalogDefaultTable;
    private final Map<ObjectPath, CatalogFunction> functions;
    private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogPartition>> partitions;

    private final Map<ObjectPath, CatalogTableStatistics> tableStats;
    private final Map<ObjectPath, CatalogColumnStatistics> tableColumnStats;
    private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogTableStatistics>> partitionStats;
    private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogColumnStatistics>>
            partitionColumnStats;


    static final String META_STRING = "STRING";
    static final String META_BOOLEAN = "BOOLEAN";
    static final String META_BYTE = "BYTE";
    static final String META_SHORT = "SMALLINT";
    static final String META_INTEGER = "INTEGER";
    static final String META_LONG = "LONG";
    static final String META_FLOAT = "FLOAT";
    static final String META_DOUBLE = "DOUBLE";
    static final String META_DATE = "DATE";
    static final String META_TIME = "TIME";
    static final String META_TIMESTAMP = "TIMESTAMP";
    static final String META_BIGINT = "BIGINT";
    static final String META_DECIMAL = "DECIMAL";
    static final String META_ARRAY = "ARRAY";
    static final String META_MAP = "MAP";
    static final String META_ROW = "STRUCT";


    public MyCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) throws ClassNotFoundException {
        super(catalogName, defaultDatabase);

        checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(pwd));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(baseUrl));
        Class.forName(MYSQL_CLASS);
        this.username = username;
        this.pwd = pwd;
        this.defaultUrl = baseUrl;

        this.tables = new LinkedHashMap<>();
        this.databases = new LinkedHashMap<>();
        this.databases.put(defaultDatabase, new CatalogDatabaseImpl(new HashMap<>(), null));
        this.catalogDefaultTable = new HashMap<>();

        this.functions = new LinkedHashMap<>();
        this.partitions = new LinkedHashMap<>();
        this.tableStats = new LinkedHashMap<>();
        this.tableColumnStats = new LinkedHashMap<>();
        this.partitionStats = new LinkedHashMap<>();
        this.partitionColumnStats = new LinkedHashMap<>();
    }

    @Override
    public void open() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            // 获取当前 db 下 table_id 列表
            List<PojoCatalogDatabase> idList = PojoCatalogDatabase.getByTableId(conn, this.getDefaultDatabase());
            for (PojoCatalogDatabase eachTable : idList) {
                int tableId = eachTable.getTableId();
                TableSchema.Builder builder = TableSchema.builder();
                // schema 组装
                for (PojoCatalogTableSchema each : PojoCatalogTableSchema.getByTableId(conn, tableId)) {
                    builder.field(each.getSchemaName(),
                            each.getSchemaRoot() == 0 ?
                                    metaTypeToDataType(each.getSchemaType()) : this.buildNestedType(each.getSchemaType()));
                }

                // 组装连接信息
                Map<String, String> properties = new HashMap<>();
                for (PojoCatalogTableConnInfo each : PojoCatalogTableConnInfo.getByTableId(conn, tableId)) {
                    properties.put(each.getKey(), each.getValue());
                }
                ObjectPath tablePath = new ObjectPath(getDefaultDatabase(), eachTable.getTableName());
                this.catalogDefaultTable.put(tablePath, false);
                this.tables.put(tablePath,
                        new CatalogTableImpl(builder.build(), properties, "").copy()
                );
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
        LOG.info("Catalog {} established connection to {}", getName(), defaultUrl);
    }

    @Override
    public void close() {
    }

    // ------ databases ------
    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        checkNotNull(database);
        if (databaseExists(databaseName)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), databaseName);
            }
        } else {
            databases.put(databaseName, database.copy());
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        if (databases.containsKey(databaseName)) {

            // Make sure the database is empty
            if (isDatabaseEmpty(databaseName)) {
                databases.remove(databaseName);
            } else if (cascade) {
                // delete all tables and functions in this database and then delete the database.
                List<ObjectPath> deleteTablePaths =
                        tables.keySet().stream()
                                .filter(op -> op.getDatabaseName().equals(databaseName))
                                .collect(Collectors.toList());
                deleteTablePaths.forEach(
                        objectPath -> {
                            try {
                                dropTable(objectPath, true);
                            } catch (TableNotExistException e) {
                                // ignore
                            }
                        });
                List<ObjectPath> deleteFunctionPaths =
                        functions.keySet().stream()
                                .filter(op -> op.getDatabaseName().equals(databaseName))
                                .collect(Collectors.toList());
                deleteFunctionPaths.forEach(
                        objectPath -> {
                            try {
                                dropFunction(objectPath, true);
                            } catch (FunctionNotExistException e) {
                                // ignore
                            }
                        });
                databases.remove(databaseName);
            } else {
                throw new DatabaseNotEmptyException(getName(), databaseName);
            }
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    private boolean isDatabaseEmpty(String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        return tables.keySet().stream().noneMatch(op -> op.getDatabaseName().equals(databaseName))
                && functions.keySet().stream()
                .noneMatch(op -> op.getDatabaseName().equals(databaseName));
    }

    @Override
    public void alterDatabase(
            String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        checkNotNull(newDatabase);

        CatalogDatabase existingDatabase = databases.get(databaseName);

        if (existingDatabase != null) {
            if (existingDatabase.getClass() != newDatabase.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Database types don't match. Existing database is '%s' and new database is '%s'.",
                                existingDatabase.getClass().getName(),
                                newDatabase.getClass().getName()));
            }

            databases.put(databaseName, newDatabase.copy());
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listDatabases() {
        return new ArrayList<>(databases.keySet());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } else {
            return databases.get(databaseName).copy();
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        return databases.containsKey(databaseName);
    }


    // ------ tables ------

    /**
     * 覆盖用户信息实现
     *
     * @param tablePath
     * @param table
     * @param ignoreIfExists
     * @throws TableAlreadyExistException
     * @throws CatalogException
     */
    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, CatalogException {

        // 注册本地表支持
        // 假如需要拦截，可以这里禁止用户自行实现连接，或者只允许 datagen 等无关数据安全的连接
        if (!this.catalogDefaultTable.containsKey(tablePath)) {
            this.tables.put(tablePath, table);
            return;
        } else if (this.catalogDefaultTable.get(tablePath)) {
            // 已经初始化，不能多次初始化
            throw new TableAlreadyExistException(getName(), tablePath);
        } else {
            this.catalogDefaultTable.put(tablePath, true);
        }

        // 元中心只有最基础的元信息
        //  以下内容需要用户自定义：watermark 策略，主键，时间属性
        // 这样做的好处，抽离元数据与 flink 核心逻辑
        TableSchema.Builder builder = TableSchema.builder();
        CatalogBaseTable metaTemplate = this.tables.get(tablePath);

        builder.fields(metaTemplate.getSchema().getFieldNames(), metaTemplate.getSchema().getFieldDataTypes());

        if (!table.getSchema().getPrimaryKey().equals(Optional.empty())) {
            builder.primaryKey(table.getSchema().getPrimaryKey().get().getColumns().toArray(new String[]{}));
        }

        if (table.getSchema().getWatermarkSpecs().size() > 0) {
            List<WatermarkSpec> tem = table.getSchema().getWatermarkSpecs();
            if (tem.size() > 1) {
                throw new IllegalStateException("Multiple watermark definition is not supported yet.");
            }
            builder.watermark(table.getSchema().getWatermarkSpecs().get(0));
        }


        // 连接信息，覆写
        Map<String, String> properties = metaTemplate.getOptions();
        Map<String, String> tem = table.getOptions();
        for (Map.Entry<String, String> each : tem.entrySet()) {
            properties.put(each.getKey(), each.getValue());
        }
        this.tables.put(tablePath,
                new CatalogTableImpl(builder.build(),
                        properties,
                        metaTemplate.getComment().equals("") ? table.getComment() : metaTemplate.getComment()
                ).copy());
    }


    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return tables.keySet().stream()
                .filter(k -> k.getDatabaseName().equals(databaseName))
                .map(k -> k.getObjectName())
                .collect(Collectors.toList());
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return tables.keySet().stream()
                .filter(k -> k.getDatabaseName().equals(databaseName))
                .filter(k -> (tables.get(k) instanceof CatalogView))
                .map(k -> k.getObjectName())
                .collect(Collectors.toList());
    }


    /**
     *
     * @param tablePath
     * @return
     * @throws TableNotExistException
     * @throws CatalogException
     */
    @SneakyThrows
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        } else {
            return tables.get(tablePath).copy();
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        return databaseExists(tablePath.getDatabaseName()) && tables.containsKey(tablePath);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }


    private DataType dfsBuild(JsonNode jsonNode) {
        String type = jsonNode.get("type").asText();
        if (type.equals(META_ARRAY) || type.equals(META_MAP) || type.equals(META_ROW)) {
            if (type.equals(META_ARRAY)) {
                return DataTypes.ARRAY(Objects.requireNonNull(this.dfsBuild(jsonNode.get("next"))));
            } else if (type.equals(META_MAP)) {
                return DataTypes.MAP(Objects.requireNonNull(this.dfsBuild(jsonNode.get("key"))),
                        Objects.requireNonNull(this.dfsBuild(jsonNode.get("value"))));
            } else {

                List<DataTypes.Field> temp = new ArrayList<>();
                for (JsonNode each : jsonNode.withArray("field")) {
                    // structName, structType
                    temp.add(DataTypes.FIELD(each.get("name").asText(),
                            Objects.requireNonNull(this.dfsBuild(each))));
                }
                return DataTypes.ROW(temp.toArray(new DataTypes.Field[]{}));
            }
        } else {
            return metaTypeToDataType(type);
        }
    }

    private DataType buildNestedType(String sou) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(sou);
        return this.dfsBuild(jsonNode);
    }


    private void ensureTableExists(ObjectPath tablePath) throws TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    private void ensurePartitionedTable(ObjectPath tablePath) throws TableNotPartitionedException {
        if (!isPartitionedTable(tablePath)) {
            throw new TableNotPartitionedException(getName(), tablePath);
        }
    }


    // ------ functions ------

    @Override
    public void createFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        checkNotNull(path);
        checkNotNull(function);

        ObjectPath functionPath = normalize(path);

        if (!databaseExists(functionPath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), functionPath.getDatabaseName());
        }

        if (functionExists(functionPath)) {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(getName(), functionPath);
            }
        } else {
            functions.put(functionPath, function.copy());
        }
    }

    @Override
    public void alterFunction(
            ObjectPath path, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        checkNotNull(path);
        checkNotNull(newFunction);

        ObjectPath functionPath = normalize(path);

        CatalogFunction existingFunction = functions.get(functionPath);

        if (existingFunction != null) {
            if (existingFunction.getClass() != newFunction.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Function types don't match. Existing function is '%s' and new function is '%s'.",
                                existingFunction.getClass().getName(),
                                newFunction.getClass().getName()));
            }

            functions.put(functionPath, newFunction.copy());
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    @Override
    public void dropFunction(ObjectPath path, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        if (functionExists(functionPath)) {
            functions.remove(functionPath);
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return functions.keySet().stream()
                .filter(k -> k.getDatabaseName().equals(databaseName))
                .map(k -> k.getObjectName())
                .collect(Collectors.toList());
    }

    @Override
    public CatalogFunction getFunction(ObjectPath path) throws FunctionNotExistException {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        if (!functionExists(functionPath)) {
            throw new FunctionNotExistException(getName(), functionPath);
        } else {
            return functions.get(functionPath).copy();
        }
    }

    @Override
    public boolean functionExists(ObjectPath path) {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        return databaseExists(functionPath.getDatabaseName())
                && functions.containsKey(functionPath);
    }

    private ObjectPath normalize(ObjectPath path) {
        return new ObjectPath(
                path.getDatabaseName(), FunctionIdentifier.normalizeName(path.getObjectName()));
    }


    // ------ partitions ------

    /**
     * Check if the given table is a partitioned table. Note that "false" is returned if the table
     * doesn't exists.
     */
    private boolean isPartitionedTable(ObjectPath tablePath) {
        CatalogBaseTable table = null;
        try {
            table = getTable(tablePath);
        } catch (TableNotExistException e) {
            return false;
        }

        return (table instanceof CatalogTable) && ((CatalogTable) table).isPartitioned();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath);

        ensureTableExists(tablePath);
        ensurePartitionedTable(tablePath);

        return new ArrayList<>(partitions.get(tablePath).keySet());
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        ensurePartitionedTable(tablePath);

        CatalogTable catalogTable = (CatalogTable) getTable(tablePath);
        List<String> partKeys = catalogTable.getPartitionKeys();
        Map<String, String> spec = partitionSpec.getPartitionSpec();
        if (!partKeys.containsAll(spec.keySet())) {
            return new ArrayList<>();
        }

        return partitions.get(tablePath).keySet().stream()
                .filter(
                        ps ->
                                ps.getPartitionSpec()
                                        .entrySet()
                                        .containsAll(partitionSpec.getPartitionSpec().entrySet()))
                .collect(Collectors.toList());
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }


    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        if (!partitionExists(tablePath, partitionSpec)) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }

        return partitions.get(tablePath).get(partitionSpec).copy();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        return partitions.containsKey(tablePath)
                && partitions.get(tablePath).containsKey(partitionSpec);
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, PartitionAlreadyExistsException,
            CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);
        checkNotNull(partition);

        ensureTableExists(tablePath);
        ensurePartitionedTable(tablePath);
        ensureFullPartitionSpec(tablePath, partitionSpec);

        if (partitionExists(tablePath, partitionSpec)) {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException(getName(), tablePath, partitionSpec);
            }
        }

        partitions.get(tablePath).put(partitionSpec, partition.copy());
    }

    private void ensureFullPartitionSpec(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, PartitionSpecInvalidException {
        if (!isFullPartitionSpec(tablePath, partitionSpec)) {
            throw new PartitionSpecInvalidException(
                    getName(),
                    ((CatalogTable) getTable(tablePath)).getPartitionKeys(),
                    tablePath,
                    partitionSpec);
        }
    }

    /**
     * Check if the given partitionSpec is full partition spec for the given table.
     */
    private boolean isFullPartitionSpec(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException {
        CatalogBaseTable baseTable = getTable(tablePath);

        if (!(baseTable instanceof CatalogTable)) {
            return false;
        }

        CatalogTable table = (CatalogTable) baseTable;
        List<String> partitionKeys = table.getPartitionKeys();
        Map<String, String> spec = partitionSpec.getPartitionSpec();

        // The size of partition spec should not exceed the size of partition keys
        return partitionKeys.size() == spec.size() && spec.keySet().containsAll(partitionKeys);
    }


    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        if (partitionExists(tablePath, partitionSpec)) {
            partitions.get(tablePath).remove(partitionSpec);
            partitionStats.get(tablePath).remove(partitionSpec);
            partitionColumnStats.get(tablePath).remove(partitionSpec);
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);
        checkNotNull(newPartition);

        if (partitionExists(tablePath, partitionSpec)) {
            CatalogPartition existingPartition = partitions.get(tablePath).get(partitionSpec);

            if (existingPartition.getClass() != newPartition.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Partition types don't match. Existing partition is '%s' and new partition is '%s'.",
                                existingPartition.getClass().getName(),
                                newPartition.getClass().getName()));
            }

            partitions.get(tablePath).put(partitionSpec, newPartition.copy());
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }


    // ------ statistics ------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        if (!isPartitionedTable(tablePath)) {
            CatalogTableStatistics result = tableStats.get(tablePath);
            return result != null ? result.copy() : CatalogTableStatistics.UNKNOWN;
        } else {
            return CatalogTableStatistics.UNKNOWN;
        }
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        CatalogColumnStatistics result = tableColumnStats.get(tablePath);
        return result != null ? result.copy() : CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        if (!partitionExists(tablePath, partitionSpec)) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }

        CatalogTableStatistics result = partitionStats.get(tablePath).get(partitionSpec);
        return result != null ? result.copy() : CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        if (!partitionExists(tablePath, partitionSpec)) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }

        CatalogColumnStatistics result = partitionColumnStats.get(tablePath).get(partitionSpec);
        return result != null ? result.copy() : CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotNull(tablePath);
        checkNotNull(tableStatistics);

        if (tableExists(tablePath)) {
            tableStats.put(tablePath, tableStatistics.copy());
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotNull(tablePath);
        checkNotNull(columnStatistics);

        if (tableExists(tablePath)) {
            tableColumnStats.put(tablePath, columnStatistics.copy());
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);
        checkNotNull(partitionStatistics);

        if (partitionExists(tablePath, partitionSpec)) {
            partitionStats.get(tablePath).put(partitionSpec, partitionStatistics.copy());
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);
        checkNotNull(columnStatistics);

        if (partitionExists(tablePath, partitionSpec)) {
            partitionColumnStats.get(tablePath).put(partitionSpec, columnStatistics.copy());
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }


    private static List<Integer> getParas(String source) {
        return Arrays.stream(source.split("[(|)|,]"))
                .filter(e -> !e.equals(""))
                .map(e -> Integer.valueOf(e))
                .collect(Collectors.toList());
    }

    public static DataType metaTypeToDataType(String source) {
        String head = source.substring(0, source.indexOf('(') > -1 ? source.indexOf('(') : source.length());
        String specMeta = source.substring(head.length());
        switch (head) {
            case META_STRING:
                return DataTypes.STRING();
            case META_BOOLEAN:
                return DataTypes.BOOLEAN();
            case META_BYTE:
                return DataTypes.BYTES();
            case META_SHORT:
                return DataTypes.SMALLINT();
            case META_INTEGER:
                return DataTypes.INT();
            case META_LONG:
            case META_BIGINT:
                return DataTypes.BIGINT();
            case META_FLOAT:
                return DataTypes.FLOAT();
            case META_DOUBLE:
                return DataTypes.DOUBLE();
            case META_DECIMAL:
                return DataTypes.DECIMAL(getParas(specMeta).get(0), getParas(specMeta).get(1));
            case META_DATE:
                return DataTypes.DATE();
            case META_TIME:
                return DataTypes.TIME();
            case META_TIMESTAMP:
                return DataTypes.TIMESTAMP(getParas(specMeta).get(0));
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Metadata type '%s' yet", source));
        }
    }
}
