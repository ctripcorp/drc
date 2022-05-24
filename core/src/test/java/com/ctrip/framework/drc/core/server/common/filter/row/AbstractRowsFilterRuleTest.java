package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class AbstractRowsFilterRuleTest extends AbstractEventTest {

    private AbstractRowsFilterRule rowsFilterRule;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        rowsFilterRule = new CustomRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.core.server.common.filter.row.CustomRowsFilterRule");
        return RowsFilterType.Custom;
    }

    @Test
    public void filterRow() throws Exception {
        RowsFilterContext rowsFilterContext = new RowsFilterContext();
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        RowsFilterResult<List<AbstractRowsEvent.Row>> res = rowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
        Assert.assertFalse(res.isNoRowFiltered());
        Assert.assertEquals(res.getRes(), result);
    }

}
