package com.ctrip.framework.drc.applier.resource;

import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.TransactionTableGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    private static final String SELECT_GTID_SET_SQL = "select `gtidset` from `drcmonitordb`.`gtid_executed` where `id` = -1 and `server_uuid` = ? for update;";

    private static final String UPDATE_GTID_SET_SQL = "update `drcmonitordb`.`gtid_executed` set `gtidset` = ? where `id` = -1 and `server_uuid` = ?;";

    private static final String INSERT_GTID_SET_SQL = "insert into `drcmonitordb`.`gtid_executed`(`id`, `server_uuid`, `gno`, `gtidset`) values(-1, ?, -1, ?);";

    private static final String COMMIT = "commit";

    private static final String ROLLBACK = "rollback";

    private static final int RETRY_TIME = 10;

    private static final int PERIOD = 60 * 5;

    private volatile int commitCount;

    private volatile int recordOppositeGtids;

    private Set<Integer> indexesToMerge = null;

    private ConcurrentHashMap<Integer, Integer> usedIndex = new ConcurrentHashMap<Integer, Integer>();

    private Object[] flags = new Object[TRANSACTION_TABLE_SIZE];

    private ConcurrentHashMap<Integer,Boolean> beginState = new ConcurrentHashMap<Integer,Boolean>(TRANSACTION_TABLE_SIZE);

    private ConcurrentHashMap<Integer,Boolean> commitState = new ConcurrentHashMap<Integer,Boolean>(TRANSACTION_TABLE_SIZE);

    private Map<Integer, String> indexAndGtid = new ConcurrentHashMap<Integer,String>();

    private GtidSet oppositeGtidSet = new GtidSet("");

    private final Object oppositeGtidSetLock = new Object();

    private long mergeGtidLastTime;

    private ExecutorService oppositeGtidService = ThreadUtils.newSingleThreadExecutor("Merge-Opposite-GtidSet");

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("Merge-Opposite-GtidSet-Schedule");

    @InstanceResource
    public DataSource dataSource;

    @Override
    protected void doInitialize() throws Exception {
        for (int i = 0; i < TRANSACTION_TABLE_SIZE; i++) {
            beginState.put(i, false);
            commitState.put(i, false);
            usedIndex.put(i, 0);
            flags[i] = new Object();
        }
        mergeTransactionTableSchedule();
    }

    public void mergeRecordsFromDB() throws SQLException {
        TransactionTableGtidReader gtidReader = new TransactionTableGtidReader();
        try (Connection connection = dataSource.getConnection()) {
            GtidSet gtidSet = gtidReader.getSpecificGtidSet(connection);
            if (StringUtils.isNotBlank(gtidSet.toString())) {
                updateGtidSetInDataBase(gtidSet);
            }
            loggerTT.info("[TT] merge records from db success, merged gtid set is: {}", gtidSet.toString());
        }
    }

    @Override
    public void begin(String gtid) throws InterruptedException {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int id = (int)(gno % TRANSACTION_TABLE_SIZE);

        synchronized (flags[id]) {
            //can use if instead, while loops only once, use loops just to avoid gitlab critical issue checking
            while (beginState.get(id)) {
                if (!commitState.get(id)) {
                    usedIndex.replace(id, usedIndex.get(id) + 1);
                    loggerTT.info("[TT] [USED] start wait, gtid is: {}, index is: {}", gtid, id);
                    flags[id].wait();
                    loggerTT.info("[TT] [USED] end wait, gtid is: {}, index is: {}", gtid, id);
                }
                if (commitState.get(id)) {
                    mergeTransactionTable(true);
                    loggerTT.info("[TT] [USED] merge transaction table, gtid is: {}, index is: {}", gtid, id);
                }
            }

            beginState.put(id, true);
            loggerTT.debug("[TT] set begin, gno is: {}, index is: {}", gno, id);
        }
    }

    private synchronized void mergeTransactionTable(boolean needRetry) {
        long start = System.currentTimeMillis();
        mergeGtidLastTime = start;
        GtidSet allGtidSetToMerge = getAllGtidSetToUpdate();
        if (needRetry) {
            Boolean res = new RetryTask<>(new updateGtidSetInDataBaseCallable(allGtidSetToMerge), RETRY_TIME).call();
            if (res == null) {
                shutdownSystem();
            }
        } else {
            try {
                updateGtidSetInDataBase(allGtidSetToMerge);
            } catch (SQLException e) {
                loggerTT.error("[TT] merge transaction table failed without retry, exception is:", e);
            }
        }

        resetBeginAndCommitStates(indexesToMerge);
        loggerTT.info("[TT] merge transaction table success, cost: {} ms", System.currentTimeMillis() - start);
    }

    private GtidSet getAllGtidSetToUpdate() {
        GtidSet gtidSetToMerge = getGtidSetToMerge();
        loggerTT.info("[TT] transaction table gtid set is: {}", gtidSetToMerge.toString());

        GtidSet oppositeGtidSetToMerge = getOppositeGtidSetToMerge();
        loggerTT.info("[TT] opposite gtid set is: {}", oppositeGtidSetToMerge.toString());
        return gtidSetToMerge.union(oppositeGtidSetToMerge);
    }

    private GtidSet getOppositeGtidSetToMerge() {
        GtidSet oppositeGtidSetToMerge;
        synchronized (oppositeGtidSetLock) {
            oppositeGtidSetToMerge = oppositeGtidSet.clone();
            oppositeGtidSet = new GtidSet("");
        }
        return oppositeGtidSetToMerge;
    }

    private void shutdownSystem() {
        try {
            system.mustShutdown();
            loggerTT.info("[TT] system shutdown when update gtid set in database");
        } catch (InterruptedException e) {
            loggerTT.error("[TT] system shutdown error when update gtid set in databas", e);
        }
    }

    private GtidSet getGtidSetToMerge() {
        GtidSet gtidSet = new GtidSet("");
        ConcurrentHashMap<Integer, String> copy = new ConcurrentHashMap<Integer, String>(indexAndGtid);
        for (Map.Entry<Integer, String> entry : copy.entrySet()) {
            gtidSet.add(entry.getValue());
            indexAndGtid.remove(entry.getKey());
        }
        indexesToMerge = copy.keySet();
        return gtidSet;
    }

    private boolean updateGtidSetInDataBase(GtidSet gtidSet) throws SQLException {
        loggerTT.info("[TT] all gtid set to merge is: {}", gtidSet.toString());
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            boolean needCommit = false;
            for (String uuid : gtidSet.getUUIDs()) {
                needCommit = true;
                String gtidSetFromDb = null;
                try (PreparedStatement statement = connection.prepareStatement(SELECT_GTID_SET_SQL)) {
                    statement.setString(1, uuid);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            gtidSetFromDb = result.getString("gtidset");
                        }
                    }
                }
                loggerTT.info("[TT] gtid set select from db is: {}", gtidSetFromDb);
                if (gtidSetFromDb == null) {
                    String uuidSetToUpdate = gtidSet.getUUIDSet(uuid).toString();
                    try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_GTID_SET_SQL)) {
                        insertStatement.setString(1, uuid);
                        insertStatement.setString(2, uuidSetToUpdate);
                        if (insertStatement.executeUpdate() != 1) {
                            throw new SQLException("[TT] insert gtid set error, affected rows not 1");
                        }
                    }
                } else {
                    String uuidSetToUpdate = new GtidSet(gtidSetFromDb).union(gtidSet).getUUIDSet(uuid).toString();
                    loggerTT.info("[TT] the final gtid set to update is: {}", uuidSetToUpdate);
                    try (PreparedStatement statement = connection.prepareStatement(UPDATE_GTID_SET_SQL)) {
                        statement.setString(1, uuidSetToUpdate);
                        statement.setString(2, uuid);
                        if (statement.executeUpdate() != 1) {
                            throw new SQLException("[TT] update gtid set error, affected rows not 1");
                        }
                    }
                }
            }
            if (needCommit) {
                try (PreparedStatement statement = connection.prepareStatement(COMMIT)){
                    statement.execute();
                }
            }
        } catch (SQLException e) {
            rollback(connection);
            loggerTT.error("[TT] update gtid set in transaction table error, exception is: {}", e.getMessage());
            markDiscard(connection);
            throw e;
        } finally {
            closeConnection(connection);
        }
        return true;
    }

    private void rollback(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(ROLLBACK)){
            statement.execute();
        } catch (Throwable e) {
            loggerTT.error("[TT] transaction.rollback() - execute: ", e);
        }
    }

    private void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            loggerTT.error("[TT] connection.close(): ", e);
        }
    }

    public void markDiscard(Connection connection) {
        try {
            if (connection != null) {
                connection.unwrap(PooledConnection.class).setDiscarded(true);
                loggerTT.warn("[TT] transaction table connection discarded");
            }
        } catch (SQLException e) {
            loggerTT.error("[TT] markDiscard() will succeed absolutely, UNLIKELY - ", e);
        }
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
        int id = (int)(gno % TRANSACTION_TABLE_SIZE);

        try (PreparedStatement updateStatement = connection.prepareStatement(UPDATE_TRANSACTION_TABLE)){
            updateStatement.setLong(1, gno);
            updateStatement.setInt(2, id);
            updateStatement.setString(3, uuid);
            if (updateStatement.executeUpdate() != 1) {
                try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_TRANSACTION_TABLE)){
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
                        loggerTT.error("[TT] UNLIKELY exception when record transaction table, shutdown the system", e);
                        shutdownSystem();
                    }
                }
            }
        }
    }

    @Override
    public void rollback(String gtid) {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int index = (int)(gno % TRANSACTION_TABLE_SIZE);
        beginState.replace(index, false);
        loggerTT.info("[TT] clear begin state: {}", index);
    }

    @Override
    public void commit(String gtid) {
        String[] uuidAndGno = gtid.split(":");
        long gno = Long.parseLong(uuidAndGno[1]);
        int index = (int)(gno % TRANSACTION_TABLE_SIZE);
        indexAndGtid.put(index, gtid);
        if (needMerged()) {
            mergeTransactionTable(true);
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
    public void recordOppositeGtid(String gtid) {
        synchronized (oppositeGtidSetLock) {
            if (++recordOppositeGtids >= TRANSACTION_TABLE_MERGE_SIZE) {
                recordOppositeGtids = 0;
                mergeOppositeGtid(true);
            }
            oppositeGtidSet.add(gtid);
        }
    }

    @Override
    public void mergeOppositeGtid(boolean needRetry) {
        oppositeGtidService.submit(new Runnable() {
            @Override
            public void run() {
                mergeTransactionTable(needRetry);
                loggerTT.info("[TT] merge opposite transaction table gtid set");
            }
        });
    }

    private void mergeTransactionTableSchedule() {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long current = System.currentTimeMillis();
                if ((current - mergeGtidLastTime)/1000 > PERIOD) {
                    mergeTransactionTable(true);
                    loggerTT.info("[TT] merge transaction table gtid set periodically");
                }
            }
        }, PERIOD, PERIOD, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public ConcurrentHashMap<Integer,Boolean> getBeginState() {
        return beginState;
    }

    class updateGtidSetInDataBaseCallable implements NamedCallable<Boolean> {

        private GtidSet gtidSet;

        public updateGtidSetInDataBaseCallable(GtidSet gtidSet) {
            this.gtidSet = gtidSet;
        }

        @Override
        public Boolean call() throws SQLException {
            return updateGtidSetInDataBase(gtidSet);
        }

        @Override
        public void afterException(Throwable t) {
            loggerTT.error("[TT] update gtid set in database error", t);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                loggerTT.error("[TT] sleep error when update gtid set in database", e);
            }
        }
    }

    @Override
    protected void doDispose() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            scheduledExecutorService = null;
        }
        if (oppositeGtidService != null) {
            oppositeGtidService.shutdown();
            oppositeGtidService = null;
        }
    }
}
