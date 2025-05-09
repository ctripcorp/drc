package com.ctrip.framework.drc.monitor.performance;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.execution.insert.SingleInsertExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.xpipe.api.monitor.Task;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName ConflictTestCase
 * @Author haodongPan
 * @Date 2023/11/6 14:55
 * @Version: $
 */
/**
 *
 CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`conflictBenchmark` (
 `id` int NOT NULL AUTO_INCREMENT,
 `drc_id_int` int NOT NULL DEFAULT '1' COMMENT '空',
 `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
 */


public class ConflictTestCase extends AbstractBenchmarkCase{


    private static int ROUND;

    private static String INSERT_SQL = "insert into bbzbbzdrcbenchmarktmpdb.conflictBenchmark (`id`,`drc_id_int`,`datachange_lasttime`) values (%d,'2', NOW());";

    static {
        ROUND = ConfigService.getInstance().getConflictBenchmarkQPS()/10;
        logger.info("{}, [Init] ROUND to {}", ConflictTestCase.class.getSimpleName(),ROUND);
    }

    protected ExecutorService executorService = Executors.newFixedThreadPool(ROUND);
    protected AtomicInteger pk = new AtomicInteger(0);
    
    @Override
    protected boolean doWrite(ReadWriteSqlOperator src) {
        return true;
    }

    @Override // 
    protected boolean doTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        // config : qps , insert
        for (int i = 0; i < ROUND; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        DefaultTransactionMonitorHolder.getInstance().logTransaction("ConflictTestCase", "Insert", new Task() {
                            @Override
                            public void go() throws Exception {
                                round(src, dst);
                            }
                        });
                    } catch (Exception e) {
                        logger.error("round error", e);
                    }
                }
            });
        }
        return true;
    }

    protected void round(ReadWriteSqlOperator src,ReadWriteSqlOperator dest) {
        String insertSql = getInsertSql();
        Execution insertExecution = new SingleInsertExecution(insertSql);
        InsertCase insertCase = new QPSTestCase(insertExecution);
        insertCase.executeInsert(dest);
        Execution insertExecution2 = new SingleInsertExecution(insertSql);
        InsertCase insertCase2 = new QPSTestCase(insertExecution2);
        insertCase2.executeInsert(src);
    }

    protected String getInsertSql() {
        int curPk = pk.getAndIncrement();
        String sql = String.format(INSERT_SQL, curPk);
        return sql;
    }
}
