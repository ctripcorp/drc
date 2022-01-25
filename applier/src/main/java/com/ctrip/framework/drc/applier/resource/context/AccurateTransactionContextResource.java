package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

/**
 * @Author Slight
 * Jul 28, 2020
 */
public class AccurateTransactionContextResource extends TransactionContextResource implements TransactionContext {

    private final Logger loggerED = LoggerFactory.getLogger("EVT DELAY");
    private final Logger loggerSC = LoggerFactory.getLogger("SQL CONFLICT");

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

    private String delayDesc() {
        long delay = fetchDelayMS();
        return "delay: " + delay + "ms " + ((delay > 100) ? "SLOW" : "");
    }

    private String gtidDesc() {
        return "(" + fetchGtid() + ") ";
    }

    private String conflictSummary(String title) {
        StringBuilder conflictSummary = new StringBuilder(title);
        getLogs().forEach((l) -> conflictSummary.append("\n").append(l));
        conflictSummary.append("\n");
        return conflictSummary.toString();
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

    private TransactionData.ApplyResult conflictAndRollback() {
        String title = "CFL R" + gtidDesc() + delayDesc();
        loggerED.info(title);
        loggerSC.info(conflictSummary(title));
        return TransactionData.ApplyResult.CONFLICT_ROLLBACK;
    }

    private TransactionData.ApplyResult conflictAndCommit() {
        String title = "CFL C" + gtidDesc() + delayDesc();
        loggerED.info(title);
        loggerSC.info(conflictSummary(title));
        return TransactionData.ApplyResult.CONFLICT_COMMIT;
    }

    public TransactionData.ApplyResult deadlock() {
        loggerED.info("FAIL" + gtidDesc() + delayDesc());
        return TransactionData.ApplyResult.DEADLOCK;
    }

    public TransactionData.ApplyResult error() {
        loggerED.info("ERR" + gtidDesc() + delayDesc());
        return TransactionData.ApplyResult.UNKNOWN;
    }
}
