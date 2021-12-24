package com.ctrip.framework.drc.monitor.performance;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.monitor.function.cases.select.SingleTableSelectCase;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.xpipe.api.monitor.Task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-06-28
 */
public class SelectTestPairCase extends AbstractBenchmarkCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    private static final String SELECT_SQL = "select * from bbzbbzdrcbenchmarktmpdb.benchmark1 order by id limit 1;";

    private static int ROUND;

    static {
        ROUND = ConfigService.getInstance().getDrcMonitorQpsRound();
        logger.info("[Init] ROUND to {}", ROUND);
    }

    private ExecutorService executorService = Executors.newFixedThreadPool(ROUND * 2);

    @Override
    protected boolean doWrite(ReadWriteSqlOperator src) {
        for (int i = 0; i < ROUND; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        DefaultTransactionMonitorHolder.getInstance().logTransaction("DefaultMonitorManager", "SELECT", new Task() {
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
        Execution selectExecution = new SingleSelectExecution(SELECT_SQL);
        SelectCase selectCase = new SingleTableSelectCase(selectExecution);
        ReadResource readResource = selectCase.executeSelect(src);

        ResultSet resultSet = readResource.getResultSet();
        try{
            resultSet.next();
            logger.info("[SELECT] id: {}", resultSet.getString(1));
        } catch (SQLException e) {
            logger.error("[SELECT] resultSet error: ", e);
        }

        try {
            readResource.close();
        } catch (Exception e) {
            logger.error("[Close] ResultSet error", e);
        }
    }

}
