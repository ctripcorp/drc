package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;

import java.sql.PreparedStatement;

/**
 * @Author Slight
 * Jul 28, 2020
 */
public class AccurateTransactionContextResource extends TransactionContextResource implements TransactionContext {

    private static final String BEGIN = "begin";

    @InstanceConfig(path = "applyMode")
    public int applyMode = 0;

    @Override
    public void begin() {
        atTrace("b");
        long start = System.nanoTime();
        try (PreparedStatement statement = connection.prepareStatement(BEGIN)){
            statement.execute();
        } catch (Throwable e) {
            lastUnbearable = e;
            logger.error("transaction.begin() - execute: ", e);
        }
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("B");
    }

    @Override
    public TransactionData.ApplyResult complete() {
        if (getLastUnbearable() != null) {
            String message = getLastUnbearable().getMessage();
            DefaultEventMonitorHolder.getInstance().logBatchEvent("alert", message, 1, 0);
            rollback();
            if (message.equals("Deadlock found when trying to get lock; try restarting transaction")) {
                return deadlock();
            }
            return error();
        }
        if (getOverwriteMap().contains(false)) {
            DefaultEventMonitorHolder.getInstance().logBatchEvent("context", "conflict & rollback", 1, 0);
            rollback();
            if (applyMode == 0) {
                setGtid(fetchGtid());
                begin();
                commit();
            }
            if (applyMode == 1) {
                begin();
                recordTransactionTable(fetchGtid());
                commit();
            }
            return conflictAndRollback();
        }
        if (getConflictMap().contains(true)) {
            DefaultEventMonitorHolder.getInstance().logBatchEvent("context", "conflict", 1, 0);
            commit();
            return conflictAndCommit();
        }
        DefaultEventMonitorHolder.getInstance().logBatchEvent("context", "commit", 1, 0);
        commit();
        return success();
    }

    private TransactionData.ApplyResult success() {
        try {
            String preMark = fetchTableKey().getTableName().equals("delaymonitor") ? "DELAY MONITOR C" : "C";
            loggerED.info("{}{}{}", preMark, gtidDesc(), delayDesc());
        } catch (Throwable t) {
            loggerED.info("{}{}", gtidDesc(), delayDesc());
        }
        return TransactionData.ApplyResult.SUCCESS;
    }

    public TransactionData.ApplyResult deadlock() {
        loggerED.info("FAIL" + gtidDesc() + delayDesc());
        return TransactionData.ApplyResult.DEADLOCK;
    }

    public TransactionData.ApplyResult error() {
        loggerED.info("ERR" + gtidDesc() + delayDesc());
        return TransactionData.ApplyResult.UNKNOWN;
    }

    @Override
    protected String contextDesc() {
        return " ACCURATE";
    }

}
