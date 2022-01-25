package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.ReportConflictActivity;
import com.ctrip.framework.drc.applier.activity.monitor.entity.ConflictTransactionLog;
import com.ctrip.framework.drc.applier.resource.TransactionTable;
import com.ctrip.framework.drc.applier.resource.condition.Progress;
import com.ctrip.framework.drc.applier.resource.context.sql.*;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.system.Derived;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.fetcher.system.Resource;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static com.ctrip.framework.drc.applier.resource.context.sql.StatementExecutorResult.TYPE.*;
import static com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType.isDatetimePrecisionType;

/**
 * @Author Slight
 * Sep 27, 2019
 */
public class TransactionContextResource extends AbstractContext
        implements TransactionContext, Resource.Dynamic,
        InsertPreparedStatementLoader,
        UpdatePreparedStatementLoader,
        SelectPreparedStatementLoader,
        DeletePreparedStatementLoader {

    private final Logger loggerS = LoggerFactory.getLogger("SQL");
    private final Logger loggerTE = LoggerFactory.getLogger("TRX END");

    private static final String SET_NEXT_GTID = "set gtid_next = '%s'";
    private static final String COMMIT = "commit";
    private static final String ROLLBACK = "rollback";

    @InstanceActivity
    public MetricsActivity metricsActivity;

    @InstanceActivity
    public ReportConflictActivity reportConflictActivity;

    @InstanceResource
    public Progress progress;

    @InstanceResource
    public TransactionTable transactionTable;

    @Derived
    public DataSource dataSource;

    //state of one operation
    private List<List<Object>> beforeRows;
    private Bitmap beforeBitmap;
    private List<List<Object>> afterRows;
    private Bitmap afterBitmap;
    private Columns columns;
    private StatementExecutorResult result;

    protected void initState(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        initState(beforeRows, beforeBitmap, null, null, columns);
    }

    private void initState(List<List<Object>> beforeRows, Bitmap beforeBitmap,
                           List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        this.beforeRows = beforeRows;
        this.beforeBitmap = beforeBitmap;
        this.afterRows = afterRows;
        this.afterBitmap = afterBitmap;
        this.columns = columns;
        this.result = StatementExecutorResult.of(UNSET);
    }

    //state of one transaction
    protected PreparedStatementExecutor executor = PreparedStatementExecutor.DEFAULT;
    protected Connection connection;
    protected TableKey tableKey;
    public static int CONFLICT_SIZE = 100;
    protected List<Boolean> conflictMap = null;
    protected List<Boolean> overwriteMap = null;
    protected Queue<String> logs;
    protected ConflictTransactionLog conflictTransactionLog;
    protected String rawSql = null;
    protected String rawSqlExecuteResult = null;
    protected String destCurrentRecord = null;
    protected String conflictHandleSql = null;
    protected String conflictHandleSqlResult = null;
    protected Throwable lastUnbearable;
    public long costTimeNS = 0;

    @Override
    public Throwable getLastUnbearable() {
        return lastUnbearable;
    }

    @Override
    public void setLastUnbearable(Throwable throwable) {
        this.lastUnbearable = throwable;
    }

    @Override
    public Queue<String> getLogs() {
        return logs;
    }

    @Override
    public void doDispose() {
        if (lastUnbearable != null) {
            markDiscard();
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("connection.close(): ", e);
        }
        logMetric();
    }

    private void logMetric() {
        try {
            String trace = endTrace("T");
            long delayMs = fetchDelayMS();
            loggerTE.info("(" + fetchGtid() + ") [" + fetchDepth() + "] cost: " + (costTimeNS / 1000) + "us"
                    + ((delayMs > 10) ? ("(" + trace + ")") : "")
                    + ((delayMs > 100) ? "SLOW" : "")
                    + ((delayMs > 1000) ? "SUPER SLOW" : ""));
            if (metricsActivity != null) {
                metricsActivity.report("trx.delay", "", delayMs);
                metricsActivity.report("transaction", tableKey != null ? tableKey.getDatabaseName() : "", 1);
                if (getOverwriteMap().contains(false)) {
                    metricsActivity.report("trx.conflict.rollback", "", 1);
                } else if (getConflictMap().contains(true)) {
                    metricsActivity.report("trx.conflict.commit", "", 1);
                }
            }

            if ((reportConflictActivity != null) && getConflictMap().contains(true)) {
                if (getOverwriteMap().contains(false)) {
                    conflictTransactionLog.setLastResult("rollback");
                } else {
                    conflictTransactionLog.setLastResult("commit");
                }
                if (!reportConflictActivity.report(conflictTransactionLog)) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.conflict.discard", tableKey.toString());
                }
            }
        } catch (Throwable t) {
            logger.error("logMetric error", t);
        }
    }

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        connection = dataSource.getConnection();
        conflictMap = new ArrayList<>(CONFLICT_SIZE);
        overwriteMap = new ArrayList<>(CONFLICT_SIZE);
        logs = new CircularFifoQueue<>(CONFLICT_SIZE);
        conflictTransactionLog = new ConflictTransactionLog();
        lastUnbearable = null;
        costTimeNS = 0;
        beginTrace("t");
        resetGtid();
    }

    @Override
    public void begin() {
        //default auto commit is now set to 0, begin packet is no longer necessary.
    }

    @Override
    public void rollback() {
        atTrace("r");
        long start = System.nanoTime();
        try (PreparedStatement statement = connection.prepareStatement(ROLLBACK)){
            statement.execute();
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.rollback() - execute: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("R");
    }

    @Override
    public void commit() {
        atTrace("c");
        long start = System.nanoTime();
        try (PreparedStatement statement = connection.prepareStatement(COMMIT)){
            statement.execute();
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.commit() - execute: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("C");
    }

    public void markDiscard() {
        try {
            if (connection != null) {
                connection.unwrap(PooledConnection.class).setDiscarded(true);
                logger.warn("(" + fetchGtid() + ") connection discarded, because: " + lastUnbearable.getMessage());
            }
        } catch (SQLException e) {
            logger.error("markDiscard() will succeed absolutely, UNLIKELY - ", e);
        }
    }

    @Override
    public void setGtid(String gtid) {
        atTrace("g");
        long start = System.nanoTime();
        try (PreparedStatement statement = connection.prepareStatement(String.format(SET_NEXT_GTID, gtid))){
            statement.execute();
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.setGtid() - execute: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("G");
    }

    public void recordTransactionTable(String gtid) {
        atTrace("tt");
        long start = System.nanoTime();
        try {
            transactionTable.record(connection, gtid);
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("update or insert transaction table failed: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("TT");
    }

    @Override
    public void setTableKey(TableKey tableKey) {
        this.tableKey = tableKey;
        updateTableKey(tableKey);
    }

    private boolean/*success, apply next row*/ insert(
            List<Object> values, Bitmap bitmap0,
            Columns columns, String comment, PreparedStatementExecutor preparedStatementExecutor
    ) throws Throwable {

        try (PreparedStatement statement = prepareInsert(
                columns.getNames(), values, bitmap0,
                comment)) {
            logRawSQL(statement, preparedStatementExecutor);
            result = preparedStatementExecutor.execute(statement);
            if (progress != null)
                progress.tick();
            logRawSQLExecutedResult(result.type.toString());
            switch (result.type) {
                case BATCHED:
                    //this row batched, do next row
                case UPDATE_COUNT_EQUALS_ONE:
                    //this row applied, do next row
                    return true;
                case UNKNOWN_COLUMN:
                    //going to remove unknown column in while() loop
                case DUPLICATE_ENTRY:
                    //going to handle insert() conflict
                    return false;
                case ERROR:
                    throw result.throwable;
                case UPDATE_COUNT_EQUALS_ZERO:
                    //auto skip
                    throw new AssertionError("0 rows updated, PROBABLY auto skipped. ");
                default:
                    throw new AssertionError("UNLIKELY, no other possibilities for result.type here");
            }
        }
    }

    private void assertDefault(Object value, Object columnDefault, int type) {
        if (value == null) {
            assert columnDefault == null
                    : "columnDefault != null";
        } else {
            if (value instanceof byte[] && columnDefault instanceof byte[]) {
                assert Arrays.equals((byte[])value, (byte[])columnDefault)
                        : "value not equals columnDefault as byte[]";
            } else {
                if (isDatetimePrecisionType(type) && StringUtils.containsIgnoreCase((String) columnDefault, "CURRENT_TIMESTAMP")) {
                    return;
                }
                assert value.equals(columnDefault)
                        : "value not equals columnDefault";
            }
        }
    }

    private String shrink() throws Throwable {
        this.columns = (Columns) columns.clone();
        this.beforeBitmap = (Bitmap) beforeBitmap.clone();
        this.beforeRows = cloneListValues(beforeRows);

        if (afterBitmap != null) {
            this.afterBitmap = (Bitmap) afterBitmap.clone();
        }
        if (afterRows != null) {
            this.afterRows = cloneListValues(afterRows);
        }
        try {
            String removed = syntaxErrorToColumnName(result.throwable);
            int index = columns.getNames().indexOf(removed);
            Object columnDefault = columns.getColumnDefaults().get(index);
            TableMapLogEvent.Column column = columns.get(index);
            beforeBitmap.remove(index);
            for (List<Object> beforeRow : beforeRows) {
                assertDefault(beforeRow.remove(index), columnDefault, column.getType());
            }
            if (afterBitmap != null && afterRows != null) {
                afterBitmap.remove(index);
                for (List<Object> afterRow : afterRows) {
                    assertDefault(afterRow.remove(index), columnDefault, column.getType());
                }
            }
            columns.removeColumn(removed);
            return removed;
        } catch (Throwable t) {
            logger.warn("fail to remove unknown column: " + t.getMessage());
            throw result.throwable;
        }
    }

    protected List<List<Object>> cloneListValues(List<List<Object>> values) {
        List<List<Object>> clone = Lists.newArrayList();
        for (int i = 0; i < values.size(); ++i) {
            List<Object> origin = values.get(i);
            List<Object> cList = Lists.newArrayList();
            for(int j = 0; j < origin.size(); ++j) {
                cList.add(origin.get(j));
            }
            clone.add(cList);
        }
        return clone;
    }

    //In most cases, update0() is used as DRC replication goes on, without conflict.
    //Always, update0() validates current last_updated_time with history of incoming data, strictly.
    //To complete a mysql update(), the last_updated_time must match the history;
    //eg.  for  [id,  name, last_updated_time] (id as PK)
    // current  [ 9,   phy, 5:00]
    // incoming [ 9, marry, 5:01] (on [ 9, 5:00])
    //                                +- this is history
    //
    //This case is seen as non-conflict by update0(), because the two "5:00"s matches.
    // -- It's quite (almost) probably that [ 9, marry, 5:01] was once applied on [ 9, phy, 5:00].
    // result   [ 9, marry, 5:01]
    //
    //
    //Otherwise,
    //eg.  for  [id,  name, last_updated_time] (id as PK)
    // current  [ 9,   phy, 5:00]
    // incoming [ 9, marry, 5:01] on [ 9, 4:49]
    //                                +- this is history
    //
    //As "4:49" and "5:00" does not match, update0() returns false, firing conflict handling.
    //
    //Note that if current row does not exist, update0() also returns false.
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean/*success, apply next row*/ update0(
            List<Object> values, Bitmap bitmap0,
            List<Object> conditions, Bitmap bitmap1,
            Columns columns, String comment, PreparedStatementExecutor preparedStatementExecutor) throws Throwable {
        try (PreparedStatement statement = prepareUpdate0(
                columns.getNames(),
                values, bitmap0,
                conditions, bitmap1,
                comment)) {
            logRawSQL(statement, preparedStatementExecutor);
            result = preparedStatementExecutor.execute(statement);
            if (progress != null)
                progress.tick();
            logRawSQLExecutedResult(result.type.toString());
            switch (result.type) {
                case BATCHED:
                    //this row batched, do next row
                case UPDATE_COUNT_EQUALS_ONE:
                    //this row applied, do next row
                    return true;
                case DUPLICATE_ENTRY:
                    //impossible for update()
                case UNKNOWN_COLUMN:
                    //going to remove unknown column once more time in while() loop
                case NO_PARAMETER:
                    //going to remove unknown column once more time in while() loop
                case UPDATE_COUNT_EQUALS_ZERO:
                    //going to handle update() conflict
                    return false;
                case ERROR:
                    throw result.throwable;
                default:
                    throw new AssertionError("UNLIKELY, no other possibilities for result.type here");
            }
        }
    }

    //The method update1() is used when insert() or update() encounter a conflict.
    //When the last_updated_time of an incoming data is larger than the corresponding current one's,
    // which means the incoming data is newer,
    // update1() will replace the current with the incoming data in spite of conflict.
    //eg.  for  [id,  name, last_updated_time] (id as PK)
    // current  [ 9,   phy, 5:00]
    // incoming [ 9, marry, 5:01]
    //
    //As 5:01 is larger than 5:00, update1() updates the row 9.
    // result   [ 9, marry, 5:01]
    //
    //
    //Otherwise,
    //eg.  for  [id,  name, last_updated_time] (id as PK)
    // current  [ 9,   phy, 5:00]
    // incoming [ 9, marry, 4:59]
    //
    //As a newer data is there yet, update1() rollbacks.
    // result   [ 9,   phy, 5:00]
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean/*success, apply next row*/ update1(
            List<Object> values, Bitmap bitmap0,
            List<Object> identifier, Bitmap bitmap1,
            List<Object> valuesOnUpdate, Bitmap bitmap2,
            Columns columns, String comment, PreparedStatementExecutor preparedStatementExecutor) throws Throwable {
        try (PreparedStatement statement = prepareUpdate1(
                columns.getNames(),
                values, bitmap0,
                identifier, bitmap1,
                valuesOnUpdate, bitmap2,
                comment
        )) {
            logConflictHandleSQL(statement, preparedStatementExecutor);
            result = preparedStatementExecutor.execute(statement);
            if (progress != null)
                progress.tick();
            logConflictHandleSQLExecutedResult(result.type.toString());
            switch (result.type) {
                case UPDATE_COUNT_EQUALS_ONE:
                    return true;
                case UPDATE_COUNT_EQUALS_ZERO:
                    return false;
                case UNKNOWN_COLUMN:
                case ERROR:
                    throw result.throwable;
                default:
                    throw new AssertionError("UNLIKELY, no other possibilities for result.type here");
            }
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    protected boolean/*success, apply next row*/ delete(
            List<Object> identifier, Bitmap bitmap0,
            Columns columns, String comment, PreparedStatementExecutor preparedStatementExecutor) throws Throwable {
        try (PreparedStatement statement = prepareDelete(
                columns.getNames(),
                identifier, bitmap0,
                comment
        )){
            logRawSQL(statement, preparedStatementExecutor);
            result = preparedStatementExecutor.execute(statement);
            if (progress != null)
                progress.tick();
            logRawSQLExecutedResult(result.type.toString());
            switch (result.type) {
                case BATCHED:
                case UPDATE_COUNT_EQUALS_ONE:
                    return true;
                case UPDATE_COUNT_EQUALS_ZERO:
                case NO_PARAMETER:
                    return false;
                case ERROR:
                case UNKNOWN_COLUMN:
                    throw result.throwable;
                case DUPLICATE_ENTRY:
                default:
                    throw new AssertionError("UNLIKELY, no other possibilities for result.type here");
            }
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private int/*row count*/ select(List<Object> identifier, Bitmap bitmap0,
                                    Columns columns, String comment, PreparedStatementExecutor preparedStatementExecutor) {
        return select(identifier, Lists.<Bitmap>newArrayList(bitmap0), columns, comment, preparedStatementExecutor);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private int/*row count*/ select(List<Object> identifier, List<Bitmap> bitmaps,
                                    Columns columns, String comment, PreparedStatementExecutor preparedStatementExecutor) {
        int rowCount = 0;
        for (Bitmap bitmap : bitmaps) {
            try (PreparedStatement statement = prepareSelect(
                    columns.getNames(),
                    identifier, bitmap,
                    comment
            )) {
                logSQL(statement, preparedStatementExecutor);
                assert statement.execute();
                try (ResultSet result = statement.getResultSet()) {
                    while (result.next()) {
                        rowCount += 1;
                        String log = "|";
                        for (String columnName : columns.getNames()) {
                            log = log + result.getString(columnName) + "|";
                        }
                        addLogs(log);
                        destCurrentRecord = log;
                        loggerS.info("(" + fetchGtid() + ")" + log);
                    }
                }
            } catch (Throwable t) {
                logger.warn("fail to select current row at conflict - IGNORE -", t);
            }
        }
        addLogs("related rows count: " + rowCount);
        if (rowCount == 0) {
            destCurrentRecord = "related rows count is 0";
        }
        return rowCount;
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        this.insert(beforeRows, beforeBitmap, columns, executor);
    }

    protected void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns, PreparedStatementExecutor preparedStatementExecutor) {
        atTrace("i");
        long start = System.nanoTime();
        if (lastUnbearable != null) {
            return;
        }
        initState(beforeRows, beforeBitmap, columns);
        try {
            STATEMENT:
            for (int i = 0; i < this.beforeRows.size(); i++) {
                if (insert(this.beforeRows.get(i), this.beforeBitmap,
                        this.columns, "DRC INSERT 0", preparedStatementExecutor)) {
                    conflictMark(false);
                    continue STATEMENT;
                }
                conflictMark(true);
                while (result.type == UNKNOWN_COLUMN) {
                    String removedColumnName = shrink();
                    if (insert(this.beforeRows.get(i), this.beforeBitmap,
                            this.columns, "DRC INSERT AFTER REMOVE " + removedColumnName, preparedStatementExecutor)) {
                        overwriteMark(true, "unknown column", rawSql, rawSqlExecuteResult);
                        continue STATEMENT;
                    }
                }
                Bitmap bitmapOfValueOnUpdate = this.columns.getLastBitmapOnUpdate();
                List<Object> valueOnUpdate = this.beforeBitmap.onBitmap(bitmapOfValueOnUpdate).on(this.beforeRows.get(i));
                for (int j = this.columns.getBitmapsOfIdentifier().size() - 1; j >= 0; j--) {
                    Bitmap bitmapOfIdentifier = this.columns.getBitmapsOfIdentifier().get(j);
                    List<Object> identifier = this.beforeBitmap.onBitmap(bitmapOfIdentifier).on(this.beforeRows.get(i));
                    if (1 == select(
                            identifier, bitmapOfIdentifier,
                            this.columns, "DRC INSERT CONFLICT", preparedStatementExecutor
                    )) {
                        if (update1(
                                this.beforeRows.get(i), this.beforeBitmap,
                                identifier, bitmapOfIdentifier,
                                valueOnUpdate, bitmapOfValueOnUpdate,
                                this.columns, "DRC INSERT 1", preparedStatementExecutor)) {
                            overwriteMark(true, destCurrentRecord, conflictHandleSql, conflictHandleSqlResult);
                            continue STATEMENT;
                        }
                    }
                }
                overwriteMark(false, destCurrentRecord, "handle conflict failed", "handle conflict failed");
            }
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.insert() - execute: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("I");
    }

    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap,
                       List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        this.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns, executor);
    }

    protected void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns, PreparedStatementExecutor preparedStatementExecutor) {
        atTrace("u");
        long start = System.nanoTime();
        if (lastUnbearable != null) {
            return;
        }
        initState(beforeRows, beforeBitmap, afterRows, afterBitmap, columns);
        try {
            STATEMENT:
            for (int i = 0; i < this.beforeRows.size(); i++) {
                Bitmap bitmapOfConditions = Bitmap.union(
                        this.columns.getBitmapsOfIdentifier().get(0),
                        this.columns.getLastBitmapOnUpdate()
                );
                List<Object> conditions = this.beforeBitmap
                        .onBitmap(bitmapOfConditions)
                        .on(this.beforeRows.get(i));
                loggerS.info("execute update0 for ({})", fetchGtid());
                if (update0(
                        this.afterRows.get(i), this.afterBitmap,
                        conditions, bitmapOfConditions,
                        this.columns, "DRC UPDATE 0", preparedStatementExecutor)) {
                    conflictMark(false);
                    continue STATEMENT;
                }
                conflictMark(true);
                while (result.type == UNKNOWN_COLUMN) {
                    String removedColumnName = shrink();
                    bitmapOfConditions = Bitmap.union(
                            this.columns.getBitmapsOfIdentifier().get(0),
                            this.columns.getLastBitmapOnUpdate()
                    );
                    conditions = this.beforeBitmap
                            .onBitmap(bitmapOfConditions)
                            .on(this.beforeRows.get(i));
                    if (update0(
                            this.afterRows.get(i), this.afterBitmap,
                            conditions, bitmapOfConditions,
                            this.columns, "DRC UPDATE AFTER REMOVE " + removedColumnName, preparedStatementExecutor)) {
                        overwriteMark(true, "unknown column", rawSql, rawSqlExecuteResult);
                        continue STATEMENT;
                    }
                }
                if (result.type == NO_PARAMETER) {
                    bitmapOfConditions = this.columns.getBitmapsOfIdentifier().get(0);
                    conditions = this.beforeBitmap
                            .onBitmap(bitmapOfConditions)
                            .on(this.beforeRows.get(i));
                    if (update0(
                            this.afterRows.get(i), this.afterBitmap,
                            conditions, bitmapOfConditions,
                            this.columns, "DRC UPDATE AFTER REMOVE WHERE ON_UPDATE=?", preparedStatementExecutor)) {
                        overwriteMark(true, "no parameter", rawSql, rawSqlExecuteResult);
                        continue STATEMENT;
                    }
                }

                Bitmap bitmap2 = this.columns.getLastBitmapOnUpdate();
                List<Object> valueOnUpdate = this.afterBitmap.onBitmap(bitmap2).on(this.afterRows.get(i));
                for (int j = this.columns.getBitmapsOfIdentifier().size() - 1; j >= 0; j--) {
                    Bitmap bitmap1 = this.columns.getBitmapsOfIdentifier().get(j);
                    List<Object> identifier = this.beforeBitmap.onBitmap(bitmap1).on(this.beforeRows.get(i));
                    if (1 == select(
                            identifier, bitmap1,
                            this.columns, "DRC UPDATE CONFLICT", preparedStatementExecutor
                    )) {
                        if (update1(
                                this.afterRows.get(i), this.afterBitmap,
                                identifier, bitmap1,
                                valueOnUpdate, bitmap2,
                                this.columns, "DRC UPDATE 1", preparedStatementExecutor)) {
                            overwriteMark(true, destCurrentRecord, conflictHandleSql, conflictHandleSqlResult);
                            continue STATEMENT;
                        }
                    }
                }
                if (insert(
                        this.afterRows.get(i), this.afterBitmap,
                        this.columns, "DRC UPDATE 2", preparedStatementExecutor)) {
                    overwriteMark(true, destCurrentRecord, rawSql, rawSqlExecuteResult);
                    continue STATEMENT;
                }
                overwriteMark(false, destCurrentRecord, "handle conflict failed", "handle conflict failed");
            }
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.update() - execute: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("U");
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        this.delete(beforeRows, beforeBitmap, columns, executor);
    }

    protected void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns, PreparedStatementExecutor preparedStatementExecutor) {
        atTrace("d");
        long start = System.nanoTime();
        if (lastUnbearable != null) {
            //record that it skips
            return;
        }
        initState(beforeRows, beforeBitmap, columns);
        try {
            STATEMENT:
            for (int i = 0; i < beforeRows.size(); i++) {
                Bitmap bitmapOfConditions = Bitmap.union(
                        columns.getBitmapsOfIdentifier().get(0),
                        columns.getLastBitmapOnUpdate()
                );
                List<Object> conditions = beforeBitmap
                        .onBitmap(bitmapOfConditions)
                        .on(beforeRows.get(i));
                if (delete(conditions, bitmapOfConditions,
                        columns, "DRC DELETE 0", preparedStatementExecutor)) {
                    conflictMark(false);
                    continue STATEMENT;
                }
                conflictMark(true);

                if (result.type == NO_PARAMETER) {
                    bitmapOfConditions = this.columns.getBitmapsOfIdentifier().get(0);
                    conditions = beforeBitmap
                            .onBitmap(bitmapOfConditions)
                            .on(beforeRows.get(i));
                    if (delete(conditions, bitmapOfConditions,
                            columns, "DRC DELETE AFTER REMOVE WHERE ON_UPDATE=?", preparedStatementExecutor)) {
                        overwriteMark(true, "no parameter", rawSql, rawSqlExecuteResult);
                        continue STATEMENT;
                    }
                }

                Bitmap bitmapOfIdentifier = columns.getBitmapsOfIdentifier().get(0);
                List<Object> identifier = beforeBitmap
                        .onBitmap(bitmapOfIdentifier)
                        .on(beforeRows.get(i));
                select(
                        identifier, bitmapOfIdentifier,
                        columns, "DRC INSERT CONFLICT", preparedStatementExecutor
                );
                overwriteMark(true, destCurrentRecord, "ignore conflict", "ignore conflict");
            }
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.delete()", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("D");
    }

    @Override
    public TransactionData.ApplyResult complete() {
        commit();
        return TransactionData.ApplyResult.SUCCESS;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public List<Boolean> getConflictMap() {
        return conflictMap;
    }

    @Override
    public List<Boolean> getOverwriteMap() {
        return overwriteMap;
    }

    protected String statementToString(PreparedStatement statement) {
        try {
            return ((com.mysql.jdbc.PreparedStatement)statement).asSql();
        } catch (SQLException e) {
            return statement.toString();
        }
    }

    private void logSQL(PreparedStatement statement, PreparedStatementExecutor preparedStatementExecutor) {
        loggerS.info("log sql for ({})", fetchGtid());
        String sql = (preparedStatementExecutor == PreparedStatementExecutor.DEFAULT ? "A" : "B")
                + statementToString(statement);
        addLogs(sql);
        loggerS.info("({}){}", fetchGtid(), sql);
    }

    private void addLogs(String log) {
        logs.add(log);
    }

    private void conflictMark(Boolean isConflict) {
        conflictMap.add(isConflict);
        if (conflictMap.size() < CONFLICT_SIZE) {
            conflictTransactionLog.getRawSqlList().add(rawSql);
            conflictTransactionLog.getRawSqlExecutedResultList().add(rawSqlExecuteResult);
            if (!isConflict) {
                conflictTransactionLog.getDestCurrentRecordList().add("0");
                conflictTransactionLog.getConflictHandleSqlList().add("0");
                conflictTransactionLog.getConflictHandleSqlExecutedResultList().add("0");
            }
        }
    }

    private void overwriteMark(Boolean isOverwrite, String destCurrentRecord, String conflictHandleSql, String conflictHandleSqlResult) {
        overwriteMap.add(isOverwrite);
        if (overwriteMap.size() < CONFLICT_SIZE) {
            conflictTransactionLog.getDestCurrentRecordList().add(destCurrentRecord);
            conflictTransactionLog.getConflictHandleSqlList().add(conflictHandleSql);
            conflictTransactionLog.getConflictHandleSqlExecutedResultList().add(conflictHandleSqlResult);
        }
    }

    private void logRawSQL(PreparedStatement statement, PreparedStatementExecutor preparedStatementExecutor) {
        logSQL(statement, preparedStatementExecutor);
        rawSql = statementToString(statement);
    }

    private void logRawSQLExecutedResult(String result) {
        addLogs(result);
        rawSqlExecuteResult = result;
    }

    private void logConflictHandleSQL(PreparedStatement statement, PreparedStatementExecutor preparedStatementExecutor) {
        logSQL(statement, preparedStatementExecutor);
        conflictHandleSql = statementToString(statement);
    }

    private void logConflictHandleSQLExecutedResult(String result) {
        addLogs(result);
        conflictHandleSqlResult = result;
    }

    private String syntaxErrorToColumnName(Throwable t) {
        String message = t.getMessage();
        int size = message.length();
        return t.getMessage().substring(16, size - 17);
    }

    public String toString() {
        try {
            return "(jdbc)" + fetchGtid();
        } catch (Throwable t) {
            return "(jdbc)unset";
        }
    }

    protected boolean everWrong() {
        return getLastUnbearable() != null || getOverwriteMap().contains(false);
    }

    @Override
    public void setPhaseName(String name) {
        previousPhaseName.set(phaseName.get());
        phaseName.set(name);
    }

    @VisibleForTesting
    public StatementExecutorResult getResult() {
        return result;
    }

}
