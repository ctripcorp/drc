package com.ctrip.framework.drc.core.server.manager;

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
    public void filterRows() throws Exception {
        RowsFilterResult<List<List<Object>>> res = dataMediaManager.filterRows(writeRowsEvent, drcTableMapLogEvent);
        Assert.assertFalse(res.isNoRowFiltered());
        Assert.assertEquals(res.getRes(), result);
    }
}