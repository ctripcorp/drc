package com.ctrip.framework.drc.core.server.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractEventTest;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class DataMediaManagerTest extends AbstractEventTest {

    private DataMediaManager dataMediaManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataMediaManager = new DataMediaManager(dataMediaConfig);
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.core.server.common.filter.row.CustomRowsFilterRule");
        return RowsFilterType.Custom;
    }

    @Test
    public void testFilterRows() throws Exception {
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        RowsFilterResult<List<List<Object>>> res = dataMediaManager.filterRows(writeRowsEvent, rowsFilterContext);
        Assert.assertFalse(res.isNoRowFiltered().noRowFiltered());
        Assert.assertEquals(res.getRes(), result);
    }

    @Test
    public void testNoFilterRule() throws Exception {
        TableMapLogEvent origin = rowsFilterContext.getDrcTableMapLogEvent();
        try {
            TableMapLogEvent drcTableMapEventOfMissed = drcTableMapEventOfMissed();
            rowsFilterContext.setDrcTableMapLogEvent(drcTableMapEventOfMissed);
            RowsFilterResult<List<List<Object>>>res = dataMediaManager.filterRows(writeRowsEvent, rowsFilterContext);
            Assert.assertTrue(res.isNoRowFiltered() == RowsFilterResult.Status.No_Filter_Rule);
        } finally {
            rowsFilterContext.setDrcTableMapLogEvent(origin);
        }
    }
}
