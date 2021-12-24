package com.ctrip.framework.drc.monitor.performance;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.select.SingleTableSelectCase;
import com.ctrip.framework.drc.monitor.function.cases.truncate.DefaultTableTruncate;
import com.ctrip.framework.drc.monitor.function.cases.truncate.TableTruncate;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.SingleInsertExecution;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.xpipe.api.monitor.Task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jixinwang on 2020/11/4
 */
public class DdlUpdateCase extends AbstractBenchmarkCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {
    private static int ROUND;
    private static boolean insertServerFlag = true;
    private static final String SELECT_BENCHMARK = "select count(*) from `bbzbbzdrcbenchmarktmpdb`.`benchmark`;";
    private static final String TRUNCATE_BENCHMARK_TABLE = "truncate table `bbzbbzdrcbenchmarktmpdb`.`benchmark`;";
    private Random rand = new Random();

    static {
        ROUND = ConfigService.getInstance().getDrcDdlQpsRound();
    }

    private ExecutorService executorService = Executors.newFixedThreadPool(ROUND * 2);

    @Override
    public void test(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());

        if (insertServerFlag) {

            if (getRecordCount(src, SELECT_BENCHMARK) != 1000) {
                TableTruncate tableTruncate = new DefaultTableTruncate();
                tableTruncate.truncateTable(src, TRUNCATE_BENCHMARK_TABLE);
                tableTruncate.truncateTable(dst, TRUNCATE_BENCHMARK_TABLE);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    logger.error("sleep error", e);
                }
                List<String> benchmarkStatements = new ArrayList<String>();

                for (int i = 1; i <= 1000; i++) {
                    String initSql = "insert into `bbzbbzdrcbenchmarktmpdb`.`benchmark` (`id`, `name`) values (" + i + ",'value');";
                    benchmarkStatements.add(initSql);
                }
                initOiSql(src, benchmarkStatements);

            }
            insertServerFlag = false;
        }

        doWrite(src);

        doTest(src, dst);
        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }

    @Override
    protected boolean doWrite(ReadWriteSqlOperator src) {
        for (int i = 0; i < ROUND; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        DefaultTransactionMonitorHolder.getInstance().logTransaction("Monitor", "Insert", new Task() {
                            @Override
                            public void go() throws Exception {
                                round(src);
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

    @Override
    protected boolean doTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        return true;
    }

    protected void round(ReadWriteSqlOperator src) {
        int id = rand.nextInt(1000) + 1;
        String updateSql = "update bbzbbzdrcbenchmarktmpdb.benchmark set datachange_lasttime=CURRENT_TIMESTAMP(3) where id=" + id + ";";
        Execution insertExecution = new SingleInsertExecution(updateSql);
        InsertCase insertCase = new QPSTestCase(insertExecution);
        insertCase.executeInsert(src);
    }


    public void initOiSql(ReadWriteSqlOperator src, List<String> statements) {
        WriteExecution writeExecution = new MultiStatementWriteExecution(statements);
        InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
        insertCase.executeBatchInsert(src);
    }

    public int getRecordCount(ReadWriteSqlOperator src, String statement) {
        Execution selectExecution = new SingleSelectExecution(statement);
        SelectCase selectCase = new SingleTableSelectCase(selectExecution);
        ReadResource expected = selectCase.executeSelect(src);
        ResultSet rs = expected.getResultSet();
        try {
            if(rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("select count error", e);
        }
        return 0;
    }
}
