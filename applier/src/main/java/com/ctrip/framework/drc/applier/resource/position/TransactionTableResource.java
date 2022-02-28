package com.ctrip.framework.drc.applier.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by jixinwang on 2021/8/23
 */
public class TransactionTableResource extends AbstractResource implements TransactionTable {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private static final int TRANSACTION_TABLE_SIZE = Integer.parseInt(System.getProperty(SystemConfig.TRANSACTION_TABLE_SIZE, SystemConfig.DEFAULT_TRANSACTION_TABLE_SIZE));

    private static final int TRANSACTION_TABLE_MERGE_SIZE = Integer.parseInt(System.getProperty(SystemConfig.TRANSACTION_TABLE_MERGE_SIZE, SystemConfig.DEFAULT_TRANSACTION_TABLE_MERGE_SIZE));

    private static final String UPDATE_TRANSACTION_TABLE = "update `drcmonitordb`.`gtid_executed` set `gno` = ? where `id`= ? and `server_uuid`= ?;";

    private static final String INSERT_TRANSACTION_TABLE = "insert into `drcmonitordb`.`gtid_executed`(`id`, `server_uuid`, `gno`) values(?, ?, ?);";

    private static final int RETRY_TIME = 10;

    private static final int MERGE_THRESHOLD = 60 * 5;

    private static final int PERIOD = 60;

    private volatile int commitCount;

    private volatile int gtidSetSizeInMemory;

    private Set<Integer> indexesToMerge = null;

    private ConcurrentHashMap<Integer, Integer> usedIndex = new ConcurrentHashMap<Integer, Integer>();

    private Object[] flags = new Object[TRANSACTION_TABLE_SIZE];

    private ConcurrentHashMap<Integer, Boolean> beginState = new ConcurrentHashMap<Integer, Boolean>(TRANSACTION_TABLE_SIZE);

    private ConcurrentHashMap<Integer, Boolean> commitState = new ConcurrentHashMap<Integer, Boolean>(TRANSACTION_TABLE_SIZE);

    private Map<Integer, String> indexAndGtid = new ConcurrentHashMap<Integer, String>();

    private GtidSet gtidSavedInMemory = new GtidSet("");

    private final Object gtidSavedInMemoryLock = new Object();

    private long lastTimeGtidMerged;

    private ExecutorService mergeGtidService = ThreadUtils.newSingleThreadExecutor("Merge-GtidSet");

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("Merge-GtidSet-Schedule");

    private SystemStatus status = SystemStatus.RUNNABLE;

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

    @Override
    protected void doInitialize() throws Exception {
        endpoint = new DefaultEndPoint(ip, port, username, password);
        dataSource = DataSourceManager.getInstance().getDataSource(endpoint);

        for (int i = 0; i < TRANSACTION_TABLE_SIZE; i++) {
            beginState.put(i, false);
            commitState.put(i, false);
            usedIndex.put(i, 0);
            flags[i] = new Object();
        }
        startGtidMergeSchedule();
    }

    @Override
    public void mergeRecord(String uuid, boolean needRetry) {
        GtidSet gtidSet = new RetryTask<>(new GtidQueryTask(uuid, endpoint), RETRY_TIME).call();
        if (gtidSet == null) {
            loggerTT.error("[TT] query gtid set error, shutdown server, key is: {}", registryKey);
            setStatus(SystemStatus.STOPPED);
        } else {
            if (StringUtils.isNotBlank(gtidSet.toString())) {
                doMergeGtid(gtidSet, needRetry);
            }
            loggerTT.info("[TT] merge gtid record in db success: {}", gtidSet.toString());
        }

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
                    loggerTT.info("[TT] [USED] start waiting, gtid is: {}, index is: {}", gtid, id);
                    flags[id].wait();
                    loggerTT.info("[TT] [USED] end waiting, gtid is: {}, index is: {}", gtid, id);
                }
                if (commitState.get(id)) {
                    loggerTT.info("[TT] [USED] merge gtid start");
                    mergeGtid(true);
                    loggerTT.info("[TT] [USED] merge gtid end, current gtid is: {}, index is: {}， commit state is: {}", gtid, id, commitState.get(id));
                }
            }

            beginState.put(id, true);
            loggerTT.debug("[TT] set begin, gno is: {}, index is: {}", gno, id);
        }
    }

    private synchronized void mergeGtid(boolean needRetry) {
        long start = System.currentTimeMillis();
        GtidSet allGtidToMerge = getAllGtidToMerge();
        doMergeGtid(allGtidToMerge, needRetry);
        resetBeginAndCommitStates(indexesToMerge);
        loggerTT.info("[TT] merge gtid success, cost: {} ms", System.currentTimeMillis() - start);
        lastTimeGtidMerged = System.currentTimeMillis();
    }

    private void doMergeGtid(GtidSet gtidSet, boolean needRetry) {
        if (needRetry) {
            Boolean res = new RetryTask<>(new GtidMergeTask(gtidSet, endpoint), RETRY_TIME).call();
            if (res == null) {
                loggerTT.error("[TT] merge gtid set error, shutdown server, key is: {}", registryKey);
                setStatus(SystemStatus.STOPPED);
            }
        } else {
            new RetryTask<>(new GtidMergeTask(gtidSet, endpoint), 0).call();
        }
    }

    private GtidSet getAllGtidToMerge() {
        GtidSet gtidSetRecorded = getGtidRecordedInDB();
        loggerTT.info("[TT] get gtid recorded to merge: {}", gtidSetRecorded.toString());

        GtidSet gtidSetInMemory = getGtidSavedInMemory();
        loggerTT.info("[TT] get gtid saved in memory to merge: {}", gtidSetInMemory.toString());

        GtidSet allGtidSetToMerge = gtidSetRecorded.union(gtidSetInMemory);
        loggerTT.info("[TT] get all gtid to merge: {}", allGtidSetToMerge.toString());
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

        try (PreparedStatement updateStatement = connection.prepareStatement(UPDATE_TRANSACTION_TABLE)) {
            updateStatement.setLong(1, gno);
            updateStatement.setInt(2, id);
            updateStatement.setString(3, uuid);
            if (updateStatement.executeUpdate() != 1) {
                try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_TRANSACTION_TABLE)) {
                    insertStatement.setInt(1, id);
                    insertStatement.setString(2, uuid);
                    insertStatement.setLong(3, gno);
                    insertStatement.execute();
                } catch (SQLException e) {
                    //already executed or deadlock
                    String message = e.getMessage();
                    if (message.startsWith("Duplicate entry") || message.equals("Deadlock found when trying to get lock; try restarting transaction")) {
                        loggerTT.error("[TT] 0 rows updated or insert for record transaction table, PROLY already executed or deadlock", e);
                        throw e;
                    } else {
                        loggerTT.error("[TT] UNLIKELY exception when record transaction table, shutdown server, key is: {}", registryKey, e);
                        setStatus(SystemStatus.STOPPED);
                    }
                }
            }
        }
    }

    @Override
    public void rollback(String gtid) {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int index = (int) (gno % TRANSACTION_TABLE_SIZE);
        beginState.replace(index, false);
        loggerTT.info("[TT] clear begin state: {}", index);
    }

    @Override
    public void commit(String gtid) {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int index = (int) (gno % TRANSACTION_TABLE_SIZE);
        indexAndGtid.put(index, gtid);
        if (needMerged()) {
            loggerTT.info("[TT] merge gtid for up to transaction table merge size {} start", commitCount);
            mergeGtid(true);
            loggerTT.info("[TT] merge gtid for up to transaction table merge size end");
        }
        setCommitState(index);
        loggerTT.debug("[TT] set commit, gno is: {}, id is: {}", gno, index);
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
                loggerTT.info("[TT] [USED] start notify, index is: {}", index);
            }
        }
    }

    @Override
    public void recordToMemory(String gtid) {
        synchronized (gtidSavedInMemoryLock) {
            if (++gtidSetSizeInMemory >= TRANSACTION_TABLE_MERGE_SIZE) {
                loggerTT.info("[TT] merge gtid for up to memory merge size {} start", gtidSetSizeInMemory);
                gtidSetSizeInMemory = 0;
                asyncMergeGtid(true);
                loggerTT.info("[TT] merge gtid for up to memory merge size end");
            }
            gtidSavedInMemory.add(gtid);
        }
    }

    public void asyncMergeGtid(boolean needRetry) {
        mergeGtidService.submit(new Runnable() {
            @Override
            public void run() {
                loggerTT.info("[TT] async merge gtid start");
                mergeGtid(needRetry);
                loggerTT.info("[TT] async merge gtid end");
            }
        });
    }

    private void startGtidMergeSchedule() {
        scheduledExecutorService.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            public void doRun() throws Exception {
                long current = System.currentTimeMillis();
                if ((current - lastTimeGtidMerged) / 1000 > MERGE_THRESHOLD) {
                    loggerTT.info("[TT] merge gtid periodically start");
                    mergeGtid(true);
                    loggerTT.info("[TT] merge gtid periodically end");
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
            loggerTT.info("[TT] merge gtid when disposing start");
            mergeGtid(false);
            loggerTT.info("[TT] merge gtid when disposing end");
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

    public SystemStatus getStatus() {
        return status;
    }

    public void setStatus(SystemStatus status) {
        this.status = status;
    }
}
