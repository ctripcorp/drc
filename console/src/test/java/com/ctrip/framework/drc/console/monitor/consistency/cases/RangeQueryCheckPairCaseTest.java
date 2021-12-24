package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.operator.StreamSqlOperatorWrapper;
import com.ctrip.framework.drc.console.monitor.consistency.table.DefaultTableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.task.KeyedQueryTask;
import com.ctrip.framework.drc.console.monitor.consistency.task.RangeQueryTask;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Set;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * Created by mingdongli
 * 2019/11/15 下午2:59.
 */
public class RangeQueryCheckPairCaseTest {

    private TableNameDescriptor tableDescriptor;

    private StreamSqlOperatorWrapper src;

    private StreamSqlOperatorWrapper dst;

    private WriteSqlOperatorWrapper srcWrite;

    private WriteSqlOperatorWrapper dstWrite;

    private RangeQueryTask rangeQueryTask;

    private KeyedQueryTask keyedQueryTask;

    private static final String OY = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(1,'shaoy', 'shaoy', DATE_SUB(NOW(), INTERVAL 75 SECOND));";
    private static final String RB = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(2,'sharb', 'sharb', DATE_SUB(NOW(), INTERVAL 76 SECOND));";
    private static final String JQ1 = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(3,'shajq1', 'shajq1', DATE_SUB(NOW(), INTERVAL 78 SECOND));";
    private static final String FQ1 = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(4,'shafq1', 'shafq1', DATE_SUB(NOW(), INTERVAL 79 SECOND));";
    private static final String JQ2 = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(3,'shajq2', 'shajq2', DATE_SUB(NOW(), INTERVAL 78 SECOND));";
    private static final String FQ2 = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(4,'shafq2', 'shafq2', DATE_SUB(NOW(), INTERVAL 79 SECOND));";

    @Before
    public void setUp() throws Exception {

        tableDescriptor = new DefaultTableNameDescriptor(TABLE, KEY, ON_UPDATE);

        rangeQueryTask = new RangeQueryTask(tableDescriptor);

        Endpoint srcEndpoint = new DefaultEndPoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        Endpoint dstEndpoint = new DefaultEndPoint(MYSQL_IP, DST_PORT, MYSQL_USER, MYSQL_PASSWORD);
        src = new StreamSqlOperatorWrapper(srcEndpoint);
        dst = new StreamSqlOperatorWrapper(dstEndpoint);
        srcWrite = new WriteSqlOperatorWrapper(srcEndpoint);
        dstWrite = new WriteSqlOperatorWrapper(dstEndpoint);

        src.initialize();
        src.start();
        dst.initialize();
        dst.start();
        srcWrite.initialize();
        srcWrite.start();
        dstWrite.initialize();
        dstWrite.start();
    }

    @Test
    public void testCompare() {
        doWrite(srcWrite, OY);
        doWrite(srcWrite, RB);
        doWrite(dstWrite, OY);
        doWrite(dstWrite, RB);
        Set<String> keys = rangeQueryTask.calculate(src, dst).keySet();
        Assert.assertEquals(0, keys.size());
        keyedQueryTask = new KeyedQueryTask(tableDescriptor, keys);
        keys = keyedQueryTask.calculate(src, dst).keySet();
        Assert.assertEquals(0, keys.size());

        doWrite(srcWrite, JQ1);
        doWrite(srcWrite, FQ1);
        doWrite(dstWrite, JQ2);
        doWrite(dstWrite, FQ2);
        keys = rangeQueryTask.calculate(src, dst).keySet();
        Assert.assertEquals(2, keys.size());
        keyedQueryTask = new KeyedQueryTask(tableDescriptor, keys);
        keys = keyedQueryTask.calculate(src, dst).keySet();
        Assert.assertEquals(2, keys.size());
    }

    public void doWrite(WriteSqlOperatorWrapper sqlOperatorWrapper, String sql) {
        GeneralSingleExecution execution = new GeneralSingleExecution(sql);
        try {
            sqlOperatorWrapper.update(execution);
        } catch (SQLException e) {
        }
    }

    @After
    public void tearDown() throws Exception {
        if (src != null) {
            src.stop();
            src.dispose();
        }
        if (dst != null) {
            dst.stop();
            dst.dispose();
        }
        if(srcWrite != null) {
            srcWrite.stop();
            srcWrite.dispose();
        }
        if(dstWrite != null) {
            dstWrite.stop();
            dstWrite.dispose();
        }
    }
}