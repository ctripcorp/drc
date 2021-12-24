package com.ctrip.framework.drc.monitor.performance;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.monitor.function.cases.select.SingleTableSelectCase;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.xpipe.tuple.Pair;

/**
 * Created by mingdongli
 * 2019/11/17 上午11:37.
 */
public class ResultCompareCase extends AbstractBenchmarkCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    public static final String INTEGRITY_TEST_DELETE = "drc.monitor.qps.delete";

    public static final String TRUNCATE_TABLE = "truncate table bbzbbzdrcbenchmarktmpdb.benchmark1;";

    private int times = 0;

    protected static int CAT_TRANSACTION;

    static {
//        CAT_TRANSACTION = Integer.parseInt(getPropertyOrDefault(INTEGRITY_TEST_DELETE, String.valueOf(2000)));
        CAT_TRANSACTION = ConfigService.getInstance().getDrcMonitorQpsDelete();
        logger.info("[Init] CAT_TRANSACTION to {}", CAT_TRANSACTION);
    }

    @Override
    protected boolean doWrite(ReadWriteSqlOperator src) {
        times++;
        return true;
    }

    @Override
    protected boolean doTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        if (times > (60 * CAT_TRANSACTION)) { //execute per 100ms，delete data after CAT_TRANSACTION minutes
            sleep(10);

            String select = "select `id`,`charlt256`,`chareq256`,`chargt256`,`varcharlt256`,`varchareq256`,`drc_id_int`,`addcol`,`addcol1`,`addcol2`,`drc_char_test_2`,`drc_tinyint_test_2`,`drc_bigint_test`,`drc_integer_test`, `drc_mediumint_test`, `drc_time6_test`, `drc_datetime3_test`, `drc_year_test`, `hourly_rate_3`, `drc_numeric10_4_test`, `drc_float_test`, `drc_double_test`, `drc_double10_4_test`, `drc_real_test`, `drc_real10_4_test`, `datachange_lasttime` from bbzbbzdrcbenchmarktmpdb.benchmark1 where `id` > %d order by id limit 250;";
            int start = 0;
            boolean result = true;
            while (true) {
                String sql = String.format(select, start);
                Execution selectExecution = new SingleSelectExecution(sql);
                SelectCase selectCase = new SingleTableSelectCase(selectExecution);
                ReadResource expected = selectCase.executeSelect(src);
                ReadResource actual = selectCase.executeSelect(dst);

                Pair<Boolean, Integer> res = toCompare(expected.getResultSet(), actual.getResultSet(), selectExecution);
                try {
                    expected.close();
                    actual.close();
                } catch (Exception e) {
                    logger.error("[Close] ResultSet error", e);
                }
                if (!res.getKey()) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DefaultMonitorManager", "Consistency-Wrong");
                    result = false;
                    break;
                } else {
                    int maxId = res.getValue();
                    if (maxId > 0) {
                        start = maxId;
                    } else {
                        DefaultEventMonitorHolder.getInstance().logEvent("DefaultMonitorManager", "Consistency-Right");
                        break; //no more
                    }
                }
            }

            times = 0; //clear
            logger.info("[Reset] times to zero");

            if (!result) {
                sleep(60);
                logger.info("[Sleep] for delaying");
            }
        }
        return true;
    }
}
