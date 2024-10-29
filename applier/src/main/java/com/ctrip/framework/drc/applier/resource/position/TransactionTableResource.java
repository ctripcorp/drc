package com.ctrip.framework.drc.applier.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.getDefaultPoolProperties;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by jixinwang on 2021/8/23
 */
public class TransactionTableResource extends AbstractResource implements TransactionTable {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    public static final int TRANSACTION_TABLE_SIZE = Integer.parseInt(System.getProperty(SystemConfig.TRANSACTION_TABLE_SIZE, SystemConfig.DEFAULT_TRANSACTION_TABLE_SIZE));

    private static final int TRANSACTION_TABLE_MERGE_SIZE = Integer.parseInt(System.getProperty(SystemConfig.TRANSACTION_TABLE_MERGE_SIZE, SystemConfig.DEFAULT_TRANSACTION_TABLE_MERGE_SIZE));

    private static final String UPDATE_TRANSACTION_SQL = "update `drcmonitordb`.`%s` set `gno` = ? where `id`= ? and `server_uuid`= ?;";
    private String UPDATE_TRANSACTION_TABLE;

    private static final String INSERT_TRANSACTION_SQL = "insert into `drcmonitordb`.`%s`(`id`, `server_uuid`, `gno`) values(?, ?, ?);";
    private String INSERT_TRANSACTION_TABLE;

    private static final int RETRY_TIME = 1;

    private static final int MERGE_THRESHOLD = 60;

    private static final int PERIOD = 5;

    private static final int SOCKET_TIMEOUT = 2000;

    private volatile int commitCount;

    private volatile int gtidSetSizeInMemory;

    private Set<Integer> indexesToMerge = null;

    private ConcurrentHashMap<Integer, Integer> usedIndex = new ConcurrentHashMap<Integer, Integer>();

    private Object[] flags = new Object[TRANSACTION_TABLE_SIZE];

    private ConcurrentHashMap<Integer, Boolean> beginState = new ConcurrentHashMap<Integer, Boolean>(TRANSACTION_TABLE_SIZE);

    private ConcurrentHashMap<Integer, Boolean> commitState = new ConcurrentHashMap<Integer, Boolean>(TRANSACTION_TABLE_SIZE);

    private Map<Integer, String> indexAndGtid = new ConcurrentHashMap<Integer, String>();

    private volatile GtidSet gtidSavedInMemory = new GtidSet("");

    private final Object gtidSavedInMemoryLock = new Object();

    private volatile long lastTimeGtidMerged;

    private ExecutorService mergeGtidService = ThreadUtils.newSingleThreadExecutor("Merge-GtidSet");

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("Merge-GtidSet-Schedule");

    private DataSource dataSource;

    private Endpoint endpoint;

    @InstanceConfig(path = "target.ip")
    public String ip;

    @InstanceConfig(path = "target.port")
    public int port;

    @InstanceConfig(path = "target.username")
    public String username;

    @InstanceConfig(path = "target.password")
    public String password;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @InstanceConfig(path = "nameFilter")
    public String nameFilter;

    @InstanceConfig(path = "applyMode")
    public int applyMode;

    @InstanceConfig(path = "includedDbs")
    public String includedDbs;

    private String trxTableName;

    @Override
    protected void doInitialize() throws Exception {
        trxTableName = getTrxTableName();
        UPDATE_TRANSACTION_TABLE = String.format(UPDATE_TRANSACTION_SQL, trxTableName);
        INSERT_TRANSACTION_TABLE = String.format(INSERT_TRANSACTION_SQL, trxTableName);

        logger.info("[transaction] update: {}, insert: {}", UPDATE_TRANSACTION_TABLE, INSERT_TRANSACTION_TABLE);

        endpoint = new DefaultEndPoint(ip, port, username, password);
        PoolProperties poolProperties = getDefaultPoolProperties(endpoint);
        String timeout = String.format("connectTimeout=%s;socketTimeout=%s", CONNECTION_TIMEOUT, SOCKET_TIMEOUT);
        poolProperties.setConnectionProperties(timeout);
        dataSource = DataSourceManager.getInstance().getDataSource(endpoint, poolProperties);

        for (int i = 0; i < TRANSACTION_TABLE_SIZE; i++) {
            beginState.put(i, false);
            commitState.put(i, false);
            usedIndex.put(i, 0);
            flags[i] = new Object();
        }
        startGtidMergeSchedule();
    }

    private String getTrxTableName() {
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applyMode)) {
            return DRC_DB_TRANSACTION_TABLE_NAME_PREFIX + includedDbs;
        } else {
            return DRC_TRANSACTION_TABLE_NAME;
        }
    }

    @Override
    public GtidSet mergeRecord(String uuid, boolean needRetry) {
        GtidSet gtidSet = new RetryTask<>(new GtidQueryTask(uuid, dataSource, registryKey, applyMode, includedDbs), RETRY_TIME).call();
        if (gtidSet == null) {
            loggerTT.error("[TT][{}] query gtid set error, shutdown server", registryKey);
            logger.info("transaction table status is stopped for {}", registryKey);
            getSystem().setStatus(SystemStatus.STOPPED);
        } else {
            if (StringUtils.isNotBlank(gtidSet.toString())) {
                doMergeGtid(gtidSet, needRetry);
            }
            loggerTT.info("[TT][{}] merge gtid record in db success: {}", registryKey, gtidSet.toString());
        }
        return gtidSet;
    }

    @Override
    public void begin(String gtid) throws InterruptedException {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int id = (int) (gno % TRANSACTION_TABLE_SIZE);

        synchronized (flags[id]) {
            //can use if instead, while loops only once, use loops just to avoid gitlab critical issue checking
            while (beginState.get(id)) {
                if (!commitState.get(id)) {
                    usedIndex.replace(id, usedIndex.get(id) + 1);
                    loggerTT.info("[TT] [USED][{}] start waiting, gtid is: {}, index is: {}", registryKey, gtid, id);
                    flags[id].wait();
                    loggerTT.info("[TT] [USED][{}] end waiting, gtid is: {}, index is: {}", registryKey, gtid, id);
                }
                if (commitState.get(id)) {
                    loggerTT.info("[TT] [USED][{}] merge gtid start", registryKey);
                    mergeGtid(true);
                    loggerTT.info("[TT] [USED][{}] merge gtid end, current gtid is: {}, index is: {}， commit state is: {}", registryKey, gtid, id, commitState.get(id));
                }
            }

            beginState.put(id, true);
            loggerTT.debug("[TT][{}] set begin, gno is: {}, index is: {}", registryKey, gno, id);
        }
    }

    private synchronized void mergeGtid(boolean needRetry) {
        long start = System.currentTimeMillis();
        GtidSet allGtidToMerge = getAllGtidToMerge();
        doMergeGtid(allGtidToMerge, needRetry);
        resetBeginAndCommitStates(indexesToMerge);
        loggerTT.info("[TT][{}] merge gtid success, cost: {} ms", registryKey, System.currentTimeMillis() - start);
        lastTimeGtidMerged = System.currentTimeMillis();
    }

    private void doMergeGtid(GtidSet gtidSet, boolean needRetry) {
        if (needRetry) {
            Boolean res = new RetryTask<>(new GtidMergeTask(gtidSet, dataSource, registryKey, trxTableName), RETRY_TIME).call();
            if (res == null) {
                loggerTT.error("[TT][{}] merge gtid set error, shutdown server", registryKey);
                logger.info("transaction table status is stopped for {}", registryKey);
                getSystem().setStatus(SystemStatus.STOPPED);
            }
        } else {
            new RetryTask<>(new GtidMergeTask(gtidSet, dataSource, registryKey, trxTableName), 0).call();
        }
    }

    private GtidSet getAllGtidToMerge() {
        GtidSet gtidSetRecorded = getGtidRecordedInDB();
        loggerTT.info("[TT][{}] get gtid recorded to merge: {}", registryKey, gtidSetRecorded.toString());

        GtidSet gtidSetInMemory = getGtidSavedInMemory();
        loggerTT.info("[TT][{}] get gtid saved in memory to merge: {}", registryKey, gtidSetInMemory.toString());

        GtidSet allGtidSetToMerge = gtidSetRecorded.union(gtidSetInMemory);
        loggerTT.info("[TT][{}] get all gtid to merge: {}", registryKey, allGtidSetToMerge.toString());
        return allGtidSetToMerge;
    }

    private GtidSet getGtidSavedInMemory() {
        GtidSet gtidSavedInMemory;
        synchronized (gtidSavedInMemoryLock) {
            gtidSavedInMemory = this.gtidSavedInMemory.clone();
            this.gtidSavedInMemory = new GtidSet("");
        }
        return gtidSavedInMemory;
    }

    private GtidSet getGtidRecordedInDB() {
        GtidSet gtidSet = new GtidSet("");
        ConcurrentHashMap<Integer, String> copy = new ConcurrentHashMap<Integer, String>(indexAndGtid);
        for (Map.Entry<Integer, String> entry : copy.entrySet()) {
            gtidSet.add(entry.getValue());
            indexAndGtid.remove(entry.getKey());
        }
        indexesToMerge = copy.keySet();
        return gtidSet;
    }

    private void resetBeginAndCommitStates(Set<Integer> idsToMerge) {
        for (Integer id : idsToMerge) {
            beginState.replace(id, false);
            commitState.replace(id, false);
        }
    }

    @Override
    public void record(Connection connection, String gtid) throws SQLException {
        String[] uuidAndGno = gtid.split(":");
        String uuid = uuidAndGno[0];
        long gno = Long.parseLong(uuidAndGno[1]);
        int id = (int) (gno % TRANSACTION_TABLE_SIZE);

        try {
            if (updateTransactionTable(connection, id, uuid, gno) != 1) {
                insertTransactionTable(connection, id, uuid, gno);
            }
        } catch (SQLException e) {
            //already executed or deadlock
            String message = e.getMessage();
            if (message.startsWith("Duplicate entry")) {
                loggerTT.info("[TT][{}] 0 rows updated or insert for record transaction table, transaction already executed, should skip: {}", registryKey, uuid + ":" + gno);
                throw new TransactionTableRepeatedUpdateException(e);
            } else if (message.equals("Deadlock found when trying to get lock; try restarting transaction")) {
                loggerTT.error("[TT][{}] 0 rows updated or insert for record transaction table, deadlock", registryKey, e);
                throw e;
            } else {
                loggerTT.error("[TT][{}] UNLIKELY exception when record transaction table, shutdown server", registryKey, e);
                logger.info("transaction table status is stopped for {}", registryKey);
                getSystem().setStatus(SystemStatus.STOPPED);
            }
        }
    }

    private int updateTransactionTable(Connection connection, int id, String uuid, long gno) throws SQLException {
        try (PreparedStatement updateStatement = connection.prepareStatement(UPDATE_TRANSACTION_TABLE)) {
            updateStatement.setLong(1, gno);
            updateStatement.setInt(2, id);
            updateStatement.setString(3, uuid);
            return updateStatement.executeUpdate();
        }
    }

    private void insertTransactionTable(Connection connection, int id, String uuid, long gno) throws SQLException {
        try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_TRANSACTION_TABLE)) {
            insertStatement.setInt(1, id);
            insertStatement.setString(2, uuid);
            insertStatement.setLong(3, gno);
            insertStatement.executeUpdate();
        }
    }

    @Override
    public void rollback(String gtid) {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int index = (int) (gno % TRANSACTION_TABLE_SIZE);
        beginState.replace(index, false);
        loggerTT.info("[TT][{}][ROLLBACK] clear begin state: {} for: {}", registryKey, index, gtid);
    }

    @Override
    public void commit(String gtid) {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int index = (int) (gno % TRANSACTION_TABLE_SIZE);
        indexAndGtid.put(index, gtid);
        if (needMerged()) {
            loggerTT.info("[TT][{}] merge gtid for up to transaction table merge size {} start", registryKey, commitCount);
            mergeGtid(true);
            loggerTT.info("[TT][{}] merge gtid for up to transaction table merge size end", registryKey);
        }
        setCommitState(index);
        loggerTT.debug("[TT][{}] set commit, gno is: {}, id is: {}", registryKey, gno, index);
    }

    private synchronized boolean needMerged() {
        if (++commitCount >= TRANSACTION_TABLE_MERGE_SIZE) {
            commitCount = 0;
            return true;
        }
        return false;
    }

    private void setCommitState(int index) {
        synchronized (flags[index]) {
            commitState.replace(index, true);
            int usedTime = usedIndex.get(index);
            if (usedTime > 0) {
                usedIndex.replace(index, usedTime - 1);
                flags[index].notify();
                loggerTT.info("[TT] [USED][{}] start notify, index is: {}", registryKey, index);
            }
        }
    }

    @Override
    public void recordToMemory(Gtid gtid) {
        synchronized (gtidSavedInMemoryLock) {
            if (++gtidSetSizeInMemory >= TRANSACTION_TABLE_MERGE_SIZE) {
                loggerTT.info("[TT][{}] merge gtid for up to memory merge size {} start", registryKey, gtidSetSizeInMemory);
                gtidSetSizeInMemory = 0;
                asyncMergeGtid(true);
                loggerTT.info("[TT][{}] merge gtid for up to memory merge size end", registryKey);
            }
            gtidSavedInMemory.add(gtid);
        }
    }

    @Override
    public void merge(GtidSet gtidSet) {
        synchronized (gtidSavedInMemoryLock) {
            gtidSavedInMemory = gtidSavedInMemory.union(gtidSet);
            asyncMergeGtid(true);
        }
    }

    public void asyncMergeGtid(boolean needRetry) {
        mergeGtidService.submit(new Runnable() {
            @Override
            public void run() {
                loggerTT.info("[TT][{}] async merge gtid start", registryKey);
                mergeGtid(needRetry);
                loggerTT.info("[TT][{}] async merge gtid end", registryKey);
            }
        });
    }

    private void startGtidMergeSchedule() {
        scheduledExecutorService.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            public void doRun() throws Exception {
                long current = System.currentTimeMillis();
                if ((current - lastTimeGtidMerged) / 1000 > MERGE_THRESHOLD) {
                    loggerTT.info("[TT][{}] merge gtid periodically start", registryKey);
                    mergeGtid(true);
                    loggerTT.info("[TT][{}] merge gtid periodically end", registryKey);
                }
            }
        }, MERGE_THRESHOLD, PERIOD, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public ConcurrentHashMap<Integer, Boolean> getBeginState() {
        return beginState;
    }

    @Override
    protected void doDispose() throws Exception {
        if (dataSource != null) {
            loggerTT.info("[TT][{}] merge gtid when disposing start", registryKey);
            mergeGtid(false);
            loggerTT.info("[TT][{}] merge gtid when disposing end", registryKey);
            DataSourceManager.getInstance().clearDataSource(endpoint);
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            scheduledExecutorService = null;
        }
        if (mergeGtidService != null) {
            mergeGtidService.shutdown();
            mergeGtidService = null;
        }
    }

    @VisibleForTesting
    public DataSource getDataSource() {
        return dataSource;
    }
}
