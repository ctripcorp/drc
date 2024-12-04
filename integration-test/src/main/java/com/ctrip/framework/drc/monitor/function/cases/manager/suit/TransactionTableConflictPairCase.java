package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.monitor.alarm.Alarm;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.SingleInsertExecution;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.List;

import static com.ctrip.framework.drc.fetcher.resource.position.TransactionTableResource.TRANSACTION_TABLE_SIZE;


public class TransactionTableConflictPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {
    private static final List<String> INSERTS = Lists.newArrayList(
            "insert into `drc1`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666661');"
    );

    private static final List<String> UPDATES = Lists.newArrayList(
            "update `drc1`.`insert1` set `three` = '3',`four` = '4' where `one` = '1';"
    );

    private static final List<String> DELETE = Lists.newArrayList(
            "delete from `drc1`.`insert1` where `one` = '1';"
    );

    private static final String SELECT_DB_DIFF = "select * from `drc1`.`insert1`;";
    private boolean needConflictTransactionTable;

    public TransactionTableConflictPairCase() {
        initResource(INSERTS, UPDATES, DELETE, SELECT_DB_DIFF);
    }

    /**
     * conflict the transaction table for first sql(drc1) for each dmlType
     */
    @Override
    protected boolean doDmlTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst, DmlTypeEnum dmlType) {
        needConflictTransactionTable = needConflictTransactionTable(dmlType);
        if (needConflictTransactionTable) {
            // 1. select src transaction table next gtid from src
            GtidSet gtidSet;
            try {
                ReadResource select = ConfigService.getInstance().getOldGtidSqlSwitch() ? src.select(new SingleSelectExecution("show global variables like \"gtid_executed\";")) : src.select(new SingleSelectExecution("SELECT @@GLOBAL.gtid_executed;"));
                ResultSet resultSet = select.getResultSet();
                if (resultSet.next()) {
                    String gtidStr = resultSet.getString(2);
                    gtidSet = new GtidSet(gtidStr);
                    if (gtidSet.getUUIDs().size() != 1) {
                        throw new Exception("cannot handle: " + gtidSet);
                    }
                } else {
                    throw new Exception("result set empty");
                }
            } catch (Exception e) {
                logger.error("select gtid fail: ", e);
                return false;
            }

            // 2. update dst transaction table, mock repeated drc transaction
            String uuid = gtidSet.getUUIDs().iterator().next();
            List<GtidSet.Interval> intervals = gtidSet.getUUIDSet(uuid).getIntervals();
            Long gno = intervals.get(intervals.size() - 1).getEnd() + 1;
            Long id = gno % (TRANSACTION_TABLE_SIZE);
            String sql = String.format("insert `drcmonitordb`.`tx_drc1` (server_uuid, id, gno) value ('%s', % d, % d) on duplicate key update `gno` = %d;", uuid, id, gno, gno);
            WriteExecution writeExecution = new SingleInsertExecution(sql);
            InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
            if (!(dst instanceof ReadWriteSqlOperator)) {
                // exception
                logger.error("unsupported: this test case need to modify dst first.");
                return false;
            }
            insertCase.executeInsert((ReadWriteSqlOperator) dst);
        }
        return super.doDmlTest(src, dst, dmlType);
    }

    private boolean needConflictTransactionTable(DmlTypeEnum dmlType) {
        return dmlType != DmlTypeEnum.DELETE;
    }

    @Override
    protected boolean diff(Execution selectExecution, ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        boolean result = super.diff(selectExecution, src, dst);
        if (needConflictTransactionTable) {
            return !result;
        }
        return result;
    }

    Alarm alarm = new Alarm() {
        @Override
        public void alarm(String content) {
            if(needConflictTransactionTable){
                return;
            }
            logger.error(">>>>>>>>>>>> [LogAlarm] {}", content);
            // skip
        }
    };

    @Override
    protected Alarm getAlarm() {
        return alarm;
    }

}
