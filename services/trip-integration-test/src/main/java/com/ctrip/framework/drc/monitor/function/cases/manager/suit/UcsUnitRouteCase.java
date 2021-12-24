package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.execution.insert.SingleInsertExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.performance.AbstractBenchmarkCase;
import com.ctrip.framework.drc.monitor.performance.QPSTestCase;
import com.ctrip.framework.ucs.client.api.RequestContext;
import com.ctrip.framework.ucs.client.api.UcsClient;
import com.ctrip.framework.ucs.client.api.UcsClientFactory;
import org.apache.commons.lang.RandomStringUtils;

public class UcsUnitRouteCase extends AbstractBenchmarkCase {

    private UcsClient ucsClient = UcsClientFactory.getInstance().getUcsClient();

    private static String sqlSeed = "insert into `test`.`customer`(`name`, `gender`, `UID`) values('testName%d', 'testGender%d', '%s');";

    private String sql;

    private static final String SRC_IDC = "ntgxy";

    private static final String DST_IDC = "ntgxh";

    @Override
    public void test(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());

        // first 50, write correctly
        for(int i = 1; i <= 50; i++) {
            String randomUidValue = RandomStringUtils.randomAlphanumeric(10);
            sql = String.format(sqlSeed, i, i, randomUidValue);

            RequestContext buildContext = ucsClient.buildRequestContext(randomUidValue, 1);
            String idc = buildContext.getRequestZone().get();
            if(SRC_IDC.equalsIgnoreCase(idc)) {
                doWrite(src);
            } else {
                doWrite(dst);
            }
        }

        // next 50, write incorrectly
        for(int i = 51; i <= 100; i++) {
            String randomUidValue = RandomStringUtils.randomAlphanumeric(10);
            sql = String.format(sqlSeed, i, i, randomUidValue);

            RequestContext buildContext = ucsClient.buildRequestContext(randomUidValue, 1);
            String idc = buildContext.getRequestZone().get();
            if(SRC_IDC.equalsIgnoreCase(idc)) {
                doWrite(dst);
            } else {
                doWrite(src);
            }
        }

        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }

    @Override
    protected boolean doWrite(ReadWriteSqlOperator src) {
        Execution insertExecution = new SingleInsertExecution(sql);
        InsertCase insertCase = new QPSTestCase(insertExecution);
        insertCase.executeInsert(src);
        return true;
    }

    @Override
    protected boolean doTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        return false;
    }
}
