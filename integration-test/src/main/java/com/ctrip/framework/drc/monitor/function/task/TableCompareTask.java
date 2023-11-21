package com.ctrip.framework.drc.monitor.function.task;


import com.ctrip.framework.drc.applier.resource.context.TransactionContextResource;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableId;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.*;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.utils.enums.StatusEnum;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.index.IndexExtractor;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbCreateTask;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbRestoreTask;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.MySQLInstance;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.wix.mysql.distribution.Version;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractColumns;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.isUsed;
import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.getDefaultPoolProperties;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;
import static com.ctrip.framework.drc.monitor.function.task.TableCompareTask.SnapshotTaskForCompare.doSnapshot;

public class TableCompareTask {
    private static final Logger logger = LoggerFactory.getLogger("TABLE COMPARE");

    private RegionConfig regionConfig = RegionConfig.getInstance();

    private static final int mysql5Port = 10007;
    private static final int mysql8Port = 10008;
    private ExecutorService executorService = ThreadUtils.newFixedThreadPool(2, "cloneTask");
    private static String schemaDir = "/opt/data/drc/schemas";

    private final AtomicBoolean running = new AtomicBoolean(false);

    public boolean compare() {
        if (!running.compareAndSet(false, true)) {
            logger.info("current running, skipping...");
            return false;
        }
        Result result = new Result();
        logger.info("--------[TASK][START]--------");
        try {
            List<DbCluster> mhaList = this.getDbClusters();
            try (MySQLWrapper mysql8 = new MySQLWrapper(mysql8Port, Version.v8_0_32);
                 MySQLWrapper mysql5 = new MySQLWrapper(mysql5Port, Version.v5_7_23)) {
                for (int i = 0; i < mhaList.size(); i++) {
                    Result mhaResult = this.compare(mysql8, mysql5, mhaList, i);
                    result.addResult(mhaResult);
                }
            }
        } catch (Throwable e) {
            logger.error("------[TASK][EXCEPTION]------", e);
        } finally {
            logger.info("------[TASK][END][CLONE FAIL:{}]------", result.getCloneFail());
            logger.info("------[TASK][END][SNAPSHOT FAIL:{}]------", result.getSnapshotFail());
            logger.info("------[TASK][END][SAME:{}][DIFF:{}][CLONE FAIL:{}][SNAPSHOT FAIL:{}]------", result.getSameCount(), result.getDiffCount(), result.getCloneFail().size(), result.getSnapshotFail().size());
            this.log("summary", ".", result.allSame());
            running.compareAndSet(true, false);
        }
        return result.allSame();
    }

    public Result compare(MySQLWrapper mysql8, MySQLWrapper mysql5, List<DbCluster> mhaList) throws InterruptedException, ExecutionException {
        Result result = new Result();
        for (int i = 0; i < mhaList.size(); i++) {
            Result mhaResult = this.compare(mysql8, mysql5, mhaList, i);
            result.addResult(mhaResult);
        }
        return result;
    }

    public Result compare(MySQLWrapper mysql8, MySQLWrapper mysql5, List<DbCluster> mhaList, int k) throws InterruptedException, ExecutionException {
        int mhaCount = k + 1;
        int mhaNum = mhaList.size();
        DbCluster dbCluster = mhaList.get(k);
        String mhaName = dbCluster.getMhaName();
        logger.info("------[MHA][START][{}][{}/{}]------", mhaName, mhaCount, mhaNum);
        Dbs dbs = dbCluster.getDbs();
        Db db = dbs.getDbs().stream().filter(Db::isMaster).findFirst().get();
        MySqlEndpoint endpoint = new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getReadUser(), dbs.getReadPassword(), true);

        logger.info("[ORIGIN][SNAPSHOT]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
        Map<String, Map<String, String>> ddlSchemas;
        try {
            ddlSchemas = tryGetLocalData(mhaName);
            if (ddlSchemas == null) {
                ddlSchemas = doSnapshot(endpoint, mhaName);
                this.saveToLocalData(mhaName, ddlSchemas);
            }
        } catch (Throwable e) {
            logger.info("[ORIGIN][SNAPSHOT][FAIL SKIP][{}]-----[mha:{}][{}/{}]", e.getMessage(), mhaName, mhaCount, mhaNum);
            Result mhaResult = new Result();
            mhaResult.addSnapshotFail(mhaName);
            return mhaResult;
        }
        boolean cleaned = cleanOriginSchemas(ddlSchemas);
        if (cleaned) {
            logger.info("[ORIGIN][SNAPSHOT][CLEANED]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
        }
        Map<String, Map<String, String>> finalDdlSchemas = ddlSchemas;
        Future<Boolean> cloneFuture5 = executorService.submit(() -> {
            try {
                logger.info("[MYSQL 5][CLONE]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
                mysql5.cloneFrom(finalDdlSchemas, mhaName);
                return true;
            } catch (Throwable e) {
                logger.info("[MYSQL 5][CLONE][FAIL SKIP][{}]-----[mha:{}][{}/{}]", e.getMessage(), mhaName, mhaCount, mhaNum);
                return false;
            }
        });

        Future<Boolean> cloneFuture8 = executorService.submit(() -> {
            try {
                logger.info("[MYSQL 8][CLONE]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
                mysql8.cloneFrom(finalDdlSchemas, mhaName);
                return true;
            } catch (Throwable e) {
                logger.info("[MYSQL 8][CLONE][FAIL SKIP][{}]-----[mha:{}][{}/{}]", e.getMessage(), mhaName, mhaCount, mhaNum);
                return false;
            }
        });
        Boolean cloneDone5 = cloneFuture5.get();
        Boolean cloneDone8 = cloneFuture8.get();
        if (!(cloneDone5 && cloneDone8)) {
            Result mhaResult = new Result();
            mhaResult.addCloneFail(mhaName);
            return mhaResult;
        }
        logger.info("[COMPARE][START]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);

        Result mhaResult = this.compareMha(mysql8, mysql5, mhaName, ddlSchemas);
        this.snapshotPerformanceTest(mysql8, mysql5, mhaName, mhaCount, mhaNum);
        logger.info("[COMPARE][MHA SAME:{}({}/{})]-----[mha:{}][{}/{}]", mhaResult.allSame(), mhaResult.getSameCount(), mhaResult.getTotalCount(), mhaName, mhaCount, mhaNum);

        return mhaResult;
    }

    private void snapshotPerformanceTest(MySQLWrapper mysql8, MySQLWrapper mysql5, String mhaName, int mhaCount, int mhaNum) throws ExecutionException, InterruptedException {
        Future<Boolean> cloneFuture5 = executorService.submit(() -> {
            try {
                long pre = System.currentTimeMillis();
                logger.info("[MYSQL 5][LOCAL_SNAPSHOT]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
                Map<String, Map<String, String>> snapshot = mysql5.snapshot(mhaName);
                logger.info("[MYSQL 5][LOCAL_SNAPSHOT][DONE:{} ms]-----[mha:{}][{}/{}]", System.currentTimeMillis() - pre, mhaName, mhaCount, mhaNum);
                pre = System.currentTimeMillis();
                logger.info("[MYSQL 5][LOCAL_SNAPSHOT_V2]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
                Map<String, Map<String, String>> snapshotNew = mysql5.snapshotNew(mhaName);
                logger.info("[MYSQL 5][LOCAL_SNAPSHOT_NEW][DONE:{} ms][SAME:{}]-----[mha:{}][{}/{}]", System.currentTimeMillis() - pre, snapshotNew.equals(snapshot), mhaName, mhaCount, mhaNum);
                return true;
            } catch (Throwable e) {
                logger.info("[MYSQL 5][LOCAL_SNAPSHOT][FAIL SKIP][{}]-----[mha:{}][{}/{}]", e.getMessage(), mhaName, mhaCount, mhaNum);
                return false;
            }
        });
        Future<Boolean> cloneFuture8 = executorService.submit(() -> {
            try {
                long pre = System.currentTimeMillis();
                logger.info("[MYSQL 8][LOCAL_SNAPSHOT]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
                Map<String, Map<String, String>> snapshot = mysql8.snapshot(mhaName);
                logger.info("[MYSQL 8][LOCAL_SNAPSHOT][DONE:{} ms]-----[mha:{}][{}/{}]", System.currentTimeMillis() - pre, mhaName, mhaCount, mhaNum);
                pre = System.currentTimeMillis();
                logger.info("[MYSQL 8][LOCAL_SNAPSHOT_V2]-----[mha:{}][{}/{}]", mhaName, mhaCount, mhaNum);
                Map<String, Map<String, String>> snapshotNew = mysql8.snapshotNew(mhaName);
                logger.info("[MYSQL 8][LOCAL_SNAPSHOT_NEW][DONE:{} ms][SAME:{}]-----[mha:{}][{}/{}]", System.currentTimeMillis() - pre, snapshotNew.equals(snapshot), mhaName, mhaCount, mhaNum);
                return true;
            } catch (Throwable e) {
                logger.info("[MYSQL 8][LOCAL_SNAPSHOT][FAIL SKIP][{}]-----[mha:{}][{}/{}]", e.getMessage(), mhaName, mhaCount, mhaNum);
                return false;
            }
        });
        cloneFuture5.get();
        cloneFuture8.get();
    }

    private Result compareMha(MySQLWrapper mysql8, MySQLWrapper mysql5, String mhaName, Map<String, Map<String, String>> ddlSchemas) {
        List<Map.Entry<String, Map<String, String>>> dbList = Lists.newArrayList(ddlSchemas.entrySet());
        int dbNum = dbList.size();
        Result mhaResult = new Result();
        for (int i = 1; i <= dbNum; i++) {
            Map.Entry<String, Map<String, String>> entry = dbList.get(i - 1);
            String dbName = entry.getKey();
            List<Map.Entry<String, String>> tableList = Lists.newArrayList(entry.getValue().entrySet());
            int tableNum = tableList.size();
            boolean dbSame = true;
            for (int j = 1; j <= tableNum; j++) {
                Map.Entry<String, String> table = tableList.get(j - 1);
                String tableName = table.getKey();
                TableInfo table8 = mysql8.find(dbName, tableName);
                TableInfo table5 = mysql5.find(dbName, tableName);
                String originCreateTable = ddlSchemas.get(dbName).get(tableName);
                Pair<Boolean, String> compare = this.compare(dbName + "." + tableName, table8, table5, originCreateTable);
                boolean same = compare.getKey();
                logger.info("[COMPARE][TABLE SAME:{}]-----[mha:{}]-------[db:{}][{}/{}]----[table:{}][{}/{}]", same, mhaName, dbName, i, dbNum, tableName, j, tableNum);
                if (same) {
                    mhaResult.addSameCount(1);
                } else {
                    logger.warn("diff:{}", compare.getValue());
                    dbSame = false;
                    mhaResult.addDiffCount(1);
                }
            }
            this.log(mhaName, dbName, dbSame);
        }
        return mhaResult;
    }

    protected List<DbCluster> getDbClusters() {
        if (!CollectionUtils.isEmpty(getCompareMha())) {
            List<String> mhaList = getCompareMha();
            return mhaList.stream().map(e -> new DbCluster().setMhaName(e).setDbs(new Dbs().addDb(new Db().setMaster(true).setIp("1.0.0.1").setPort(3306)))).collect(Collectors.toList());
        }
        Drc drc = Objects.requireNonNull(this.getDrcFromRemote(), "drc is null");
        List<DbCluster> mhaList = Lists.newArrayList();
        for (Dc value : drc.getDcs().values()) {
            mhaList.addAll(value.getDbClusters().values());
        }
        List<String> excludeCompareMha = ConfigService.getInstance().getExcludeCompareMha();
        mhaList = mhaList.stream()
                .filter(e -> !CollectionUtils.isEmpty(e.getReplicators()))
                .filter(e -> !excludeCompareMha.contains(e.getMhaName()))
                .filter(e -> e.getDbs().getDbs().stream().anyMatch(Db::isMaster))
                .collect(Collectors.toList());
        return mhaList;
    }

    private List<String> getCompareMha() {
        return ConfigService.getInstance().getCompareMha();
    }

    private boolean cleanOriginSchemas(Map<String, Map<String, String>> ddlSchemas) {
        boolean removed = false;
        for (Map.Entry<String, Map<String, String>> dbEntry : ddlSchemas.entrySet()) {
            Map<String, String> map = dbEntry.getValue();
            removed = removed || map.entrySet().removeIf(tableEntry -> tableEntry.getValue().contains("utf8mb4_0900_ai_ci"));
        }
        return removed;
    }


    private void log(String mha, String db, boolean same) {
        Transaction t = Cat.newTransaction("tbl.compare.test.case", String.join(".", mha, db));
        try {

            if (same) {
                t.addData("compare", StatusEnum.SUCCESS.getDescription());
            } else {
                t.addData("compare", StatusEnum.FAIL.getDescription());
                t.setStatus(new Exception("not same"));
            }
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            t.setStatus(e);
        } finally {
            t.complete();
        }
    }

    private Pair<Boolean, String> compare(String key, TableInfo table8, TableInfo table5, String createTable) {
        table8.getColumnList().forEach(e -> {
                    if (e.getCharset() != null) {
                        e.setCharset(e.getCharset().replace("utf8mb3", "utf8"));
                    }
                    if (e.getCollation() != null) {
                        e.setCollation(e.getCollation().replace("utf8mb3", "utf8"));
                    }
                }
        );
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isEmpty(table5.getColumnList()) && CollectionUtils.isEmpty(table8.getColumnList())) {
            return Pair.of(false, sb.append("all null").toString());
        }
        boolean dbSame = isDbSame(table8.getDbName(), table5.getDbName(), sb);
        boolean tableSame = isTableSame(table8.getTableName(), table5.getTableName(), sb);
        boolean IdentifiersSame = isIdentifiersSame(table8.getIdentifiers(), table5.getIdentifiers(), sb);
        boolean ColumnListSame = isColumnListSame(table8.getColumnList(), table5.getColumnList(), sb);
        boolean same = dbSame && tableSame && ColumnListSame && IdentifiersSame;
        if (same) {
            return Pair.of(true, null);
        }
        sb.append(createTable).append("\n");
        sb.insert(0, key + "\n");
        return Pair.of(false, sb.toString());
    }

    protected static boolean isColumnListSame(List<TableMapLogEvent.Column> columnList8, List<TableMapLogEvent.Column> columnList5, StringBuilder sb) {
        if (columnList5.size() != columnList8.size()) {
            sb.append("columnList size diff\n");
            sb.append("columnList:").append(columnList8).append("\n");
            sb.append("columnList:").append(columnList5).append("\n");
            return false;
        }
        int size = columnList5.size();
        boolean same = true;
        for (int i = 0; i < size; i++) {
            TableMapLogEvent.Column column5 = columnList5.get(i);
            TableMapLogEvent.Column column8 = columnList8.get(i);
            if (!(compareDefaultValue(column5, column8) && compareExceptDefaultValue(column5, column8))) {
                sb.append(String.format("column %d: \nmysql5: %s\nmysql8: %s\n", i, column5, column8));
                same = false;
            }
        }
        return same;
    }

    private static boolean compareDefaultValue(TableMapLogEvent.Column column5, TableMapLogEvent.Column column8) {
        if (Objects.equals(column5.getColumnDefault(), column8.getColumnDefault())) {
            return true;
        }
        // compare default object
        try {
            TransactionContextResource.assertDefault(column5.getColumnDefaultObject(), column8.getColumnDefaultObject(), column5.getType());
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    public static boolean compareExceptDefaultValue(TableMapLogEvent.Column c1, TableMapLogEvent.Column c2) {
        if (c1 == c2) return true;
        if (c1 == null || c2 == null) return false;

        return c1.getType() == c2.getType() &&
                c1.getMeta() == c2.getMeta() &&
                c1.isNullable() == c2.isNullable() &&
                c1.isUnsigned() == c2.isUnsigned() &&
                c1.isBinary() == c2.isBinary() &&
                c1.isPk() == c2.isPk() &&
                c1.isUk() == c2.isUk() &&
                c1.isOnUpdate() == c2.isOnUpdate() &&
                Objects.equals(c1.getName(), c2.getName()) &&
                Objects.equals(c1.getCharset(), c2.getCharset()) &&
                Objects.equals(c1.getCollation(), c2.getCollation());
    }


    protected static boolean isIdentifiersSame(List<List<String>> identifiers8, List<List<String>> identifiers5, StringBuilder sb) {
        boolean identifiersSame = compareIdentifiers(identifiers8, identifiers5);
        if (!identifiersSame) {
            sb.append("identifiers:").append(identifiers8).append("\n");
            sb.append("identifiers:").append(identifiers5).append("\n");
        }
        return identifiersSame;
    }

    private static boolean compareIdentifiers(List<List<String>> identifiers8, List<List<String>> identifiers5) {
        if (identifiers5 == null || identifiers8 == null) {
            return false;
        }
        if (identifiers5.size() != identifiers8.size()) {
            return false;
        }
        int size = identifiers5.size();
        if (size == 0) {
            return true;
        }
        boolean pkEquals = identifiers5.get(0).equals(identifiers8.get(0));
        if (!pkEquals) {
            return false;
        }
        // other ignore ordering
        HashSet<List<String>> set5 = Sets.newHashSet(identifiers5.subList(1, identifiers5.size()));
        HashSet<List<String>> set8 = Sets.newHashSet(identifiers8.subList(1, identifiers8.size()));
        return set5.equals(set8);
    }

    protected static boolean isTableSame(String tableName8, String tableName5, StringBuilder sb) {
        boolean tableSame = Objects.equals(tableName8, tableName5);
        if (!tableSame) {
            sb.append("table:").append(tableName8).append("\n");
            sb.append("table:").append(tableName5).append("\n");
        }
        return tableSame;
    }

    protected static boolean isDbSame(String dbName8, String dbName5, StringBuilder sb) {
        boolean dbSame = Objects.equals(dbName8, dbName5);
        if (!dbSame) {
            sb.append("db:").append(dbName8).append("\n");
            sb.append("db:").append(dbName5).append("\n");
        }
        return dbSame;
    }

    public Drc getDrcFromRemote() {
        String centerRegionUrl = regionConfig.getConsoleRegionUrls().get("sha");
        if (StringUtils.isEmpty(centerRegionUrl)) {
            logger.warn("no center region url found, skip get drc from remote");
            return null;
        }
        try {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.meta.update", "remote", () -> {
                String url = String.format("%s/api/drc/v2/meta/?refresh=true", centerRegionUrl);
                logger.info("get from remote, url {}", url);
                String drcFromRemote = HttpUtils.get(url, String.class);
                return DefaultSaxParser.parse(drcFromRemote);
            });
        } catch (Throwable t) {
            META_LOGGER.warn("Fail get drc from remote center region ", t);
        }
        return null;
    }

    public static class MySQLWrapper implements Closeable {

        public static final String host = "127.0.0.1";

        public static final String user = "root";

        public static final String password = "";
        private final String registerKey = "table_compare.test";

        public int port;
        public MySQLInstance embeddedDb;
        public Endpoint inMemoryEndpoint;
        public DataSource inMemoryDataSource;
        private final Version version;

        public MySQLWrapper(int port, Version version) {
            this.port = port;
            this.version = version;
            embeddedDb = isUsed(port)
                    ? new RetryTask<>(new DbRestoreTask(port, registerKey, version)).call()
                    : new RetryTask<>(new DbCreateTask(port, registerKey, version)).call();

            inMemoryEndpoint = new DefaultEndPoint(host, port, user, password);
            PoolProperties poolProperties = getDefaultPoolProperties(inMemoryEndpoint);
            String timeout = String.format("connectTimeout=%s;socketTimeout=%s", CONNECTION_TIMEOUT, 120000);
            poolProperties.setConnectionProperties(timeout);
            inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint, poolProperties);
            afterStart(version);
        }

        private void afterStart(Version targetVersion) {
            if (targetVersion == Version.v8_0_32) {
                setDefaultCollationForUtf8mb4();
            }
        }

        @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
        private void setDefaultCollationForUtf8mb4() {
            try (Connection connection = inMemoryDataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("set session default_collation_for_utf8mb4=utf8mb4_general_ci;");
                statement.execute("set global default_collation_for_utf8mb4=utf8mb4_general_ci;");
                statement.execute("set global innodb_print_all_deadlocks=on;");
                statement.execute("set global innodb_flush_neighbors=1;");

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (embeddedDb != null) {
                embeddedDb.destroy();
            }
        }

        public void cloneFrom(Map<String, Map<String, String>> ddlSchemas, String mha) throws Exception {
            DefaultTransactionMonitorHolder.getInstance().logTransaction("tbl.compare.clone." + version.getMajorVersion(), mha, () -> {
                Boolean schemaClear;
                try {
                    schemaClear = new RetryTask<>(new SchemeClearTask(inMemoryEndpoint, inMemoryDataSource), 20).call();
                } catch (Throwable e) {
                    throw new Exception("clean fail: " + e.getMessage());
                }
                if (!Boolean.TRUE.equals(schemaClear)) {
                    throw new Exception("clean fail!");
                }
                Boolean clone;
                try {
                    clone = new SchemeCloneTask(ddlSchemas, inMemoryEndpoint, inMemoryDataSource, registerKey).call();
                } catch (Throwable e) {
                    throw new Exception("clone fail: " + e.getMessage());
                }
                if (!Boolean.TRUE.equals(clone)) {
                    throw new Exception("clone fail!");
                }
            });
        }

        public Map<String, Map<String, String>> snapshot(String mha) throws Exception {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("tbl.compare.localsnapshot." + version.getMajorVersion(), mha, () -> {
                try {
                    return new SchemaSnapshotTask(inMemoryEndpoint, inMemoryDataSource).call();
                } catch (Throwable e) {
                    throw new Exception("snapshot fail: " + e.getMessage());
                }
            });
        }

        public Map<String, Map<String, String>> snapshotNew(String mha) throws Exception {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("tbl.compare.localsnapshot.v2." + version.getMajorVersion(), mha, () -> {
                try {
                    return new SchemaSnapshotTaskV2(inMemoryEndpoint, inMemoryDataSource).call();
                } catch (Throwable e) {
                    throw new Exception("snapshot fail: " + e.getMessage());
                }
            });
        }

        public TableInfo find(String schema, String table) {
            TableId keys = new TableId(schema, table);
            Map<TableId, TableInfo> tableInfoMap = Maps.newConcurrentMap();
            TableInfo tableMeta = tableInfoMap.get(keys);
            if (tableMeta == null) {
                synchronized (this) {
                    tableMeta = tableInfoMap.get(keys);
                    if (tableMeta == null) {
                        tableMeta = queryTableInfoByIS(inMemoryDataSource, schema, table);

                        if (tableMeta != null) {
                            tableMeta.setDbName(schema);
                            tableMeta.setTableName(table);
                            tableInfoMap.put(keys, tableMeta);
                        }
                    }
                }
            }

            return tableMeta;
        }

        private static final String INFORMATION_SCHEMA_QUERY = "select * from information_schema.COLUMNS where `TABLE_SCHEMA`=\"%s\" and `TABLE_NAME`=\"%s\" order by `ORDINAL_POSITION`";
        public static final String INDEX_QUERY = "SELECT INDEX_NAME,COLUMN_NAME FROM information_schema.statistics WHERE `table_schema` = \"%s\" AND `table_name` = \"%s\" and NON_UNIQUE=0 ORDER BY SEQ_IN_INDEX;";


        @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
        public TableInfo queryTableInfoByIS(DataSource dataSource, String schema, String table) {
            String query = String.format(INFORMATION_SCHEMA_QUERY, schema, table);
            boolean mysql8 = version == Version.v8_0_32;
            try (Connection connection = dataSource.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery(query)) {
                        TableInfo tableInfo = extractColumns(resultSet, mysql8);
                        String indexQuery = String.format(INDEX_QUERY, schema, table);
                        try (ResultSet indexResultSet = statement.executeQuery(indexQuery)) {
                            List<List<String>> indexes = IndexExtractor.extractIndex(indexResultSet);
                            tableInfo.setIdentifiers(indexes);
                        }
                        if (mysql8) {
                            Set<String> ukColumn = tableInfo.getIdentifiers().stream().filter(e -> e != null && e.size() == 1).map(e -> e.get(0)).collect(Collectors.toSet());
                            for (TableMapLogEvent.Column column : tableInfo.getColumnList()) {
                                if (!column.isPk() && ukColumn.contains(column.getName())) {
                                    column.setUk(true);
                                }
                            }
                        }
                        return tableInfo;
                    }
                }
            } catch (SQLException e) {
                logger.error("queryTableInfoByIS for {}.{} error", schema, table, e);
            }

            return null;
        }
    }

    public Map<String, Map<String, String>> tryGetLocalData(String mhaName) {
        String file = String.format("%s/%s.json", schemaDir, mhaName);
        try (FileInputStream input = new FileInputStream(file)) {
            String json = IOUtils.toString(input, StandardCharsets.UTF_8);
            return JsonUtils.fromJson(json, Map.class);
        } catch (Throwable e) {
            return null;
        }
    }

    public void saveToLocalData(String mhaName, Map<String, Map<String, String>> schemasMap) throws IOException {
        Files.createDirectories(Paths.get(schemaDir));
        String file = String.format("%s/%s.json", schemaDir, mhaName);
        try (FileWriter out = new FileWriter(file);
             BufferedWriter writer = new BufferedWriter(out)) {
            writer.write(JsonUtils.toJson(schemasMap));
            writer.flush();
        }
    }

    public static class SnapshotTaskForCompare {
        protected static Map<String, Map<String, String>> doSnapshot(Endpoint endpoint, String mha) throws Exception {
            DataSource dataSource = DataSourceManager.getInstance().getDataSource(endpoint);
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("tbl.compare.snapshot", mha, () -> {
                Map<String, Map<String, String>> snapshot = new SchemaSnapshotTask(endpoint, dataSource).call();
                if (CollectionUtils.isEmpty(snapshot)) {
                    throw new Exception("snapshot empty for" + mha);
                }
                return snapshot;
            });
        }
    }

    public static class Result {
        private int sameCount = 0;
        private int diffCount = 0;

        private final Set<String> snapshotFail = new HashSet<>();
        private final Set<String> cloneFail = new HashSet<>();


        public void addSameCount(int count) {
            sameCount += count;
        }

        public void addDiffCount(int count) {
            diffCount += count;
        }

        public int getSameCount() {
            return sameCount;
        }

        public int getTotalCount() {
            return sameCount + diffCount;
        }

        public int getDiffCount() {
            return diffCount;
        }

        public boolean allSame() {
            return diffCount == 0;
        }

        public void addSnapshotFail(String mha) {
            snapshotFail.add(mha);
        }

        public void addSnapshotFail(Set<String> mha) {
            snapshotFail.addAll(mha);
        }

        public Set<String> getSnapshotFail() {
            return snapshotFail;
        }

        public void addCloneFail(Set<String> mha) {
            cloneFail.addAll(mha);
        }

        public void addCloneFail(String mha) {
            cloneFail.add(mha);
        }

        public Set<String> getCloneFail() {
            return cloneFail;
        }

        public void addResult(Result mhaResult) {
            this.addSameCount(mhaResult.getSameCount());
            this.addDiffCount(mhaResult.getDiffCount());
            this.addSnapshotFail(mhaResult.getSnapshotFail());
            this.addCloneFail(mhaResult.getCloneFail());
        }
    }
}