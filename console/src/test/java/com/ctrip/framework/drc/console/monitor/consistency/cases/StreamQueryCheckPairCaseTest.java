package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.operator.StreamSqlOperatorWrapper;
import com.ctrip.framework.drc.console.monitor.consistency.table.DefaultTableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.task.KeyedQueryTask;
import com.ctrip.framework.drc.console.monitor.consistency.task.RangeQueryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

public class StreamQueryCheckPairCaseTest {

    private TableNameDescriptor tableDescriptor;

    private StreamSqlOperatorWrapper src;

    private StreamSqlOperatorWrapper dst;

    private RangeQueryTask rangeQueryTask;

    private KeyedQueryTask keyedQueryTask;

    @Before
    public void setUp() throws Exception {

        tableDescriptor = new DefaultTableNameDescriptor(TABLE, KEY, ON_UPDATE);

        rangeQueryTask = new RangeQueryTask(tableDescriptor);

        Endpoint srcEndpoint = new DefaultEndPoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        Endpoint dstEndpoint = new DefaultEndPoint(MYSQL_IP, DST_PORT, MYSQL_USER, MYSQL_PASSWORD);
        src = new StreamSqlOperatorWrapper(srcEndpoint);
        dst = new StreamSqlOperatorWrapper(dstEndpoint);

        src.initialize();
        src.start();
        dst.initialize();
        dst.start();
    }

    @Test
    public void testCompare() {
        Set<String> keys = rangeQueryTask.calculate(src, dst).keySet();
        Assert.assertEquals(2, keys.size());
        keyedQueryTask = new KeyedQueryTask(tableDescriptor, keys);
        keys = keyedQueryTask.calculate(src, dst).keySet();
        Assert.assertEquals(2, keys.size());
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
    }
}
