package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class UidRowsFilterRuleTest extends AbstractEventTest {

    private UidRowsFilterRule uidRowsFilterRule;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<RowsFilterConfig> rowsFilterConfigList = dataMediaConfig.getRowsFilters();
        RowsFilterConfig rowsFilterConfig = rowsFilterConfigList.get(0);
        uidRowsFilterRule = new UidRowsFilterRule(rowsFilterConfig);
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        return RowsFilterType.TripUdl;
    }

    @Test
    public void filterRows() throws Exception {
        // LocalUidService
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        RowsFilterResult<List<AbstractRowsEvent.Row>> res = uidRowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
        Assert.assertTrue(res.isNoRowFiltered().noRowFiltered());
    }

    // id with 18, 20, 22
    @Test
    public void filterRowsWithBlackList() throws Exception {
        List<RowsFilterConfig> rowsFilterConfigList = dataMediaConfig.getRowsFilters();
        RowsFilterConfig rowsFilterConfig = rowsFilterConfigList.get(0);
        RowsFilterConfig clone = getRowsFilterConfig(rowsFilterConfig, FetchMode.BlackList.getCode());

        // black 20,21,22
        UidRowsFilterRule uidRowsFilterRule = new UidRowsFilterRule(clone);
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        RowsFilterResult<List<AbstractRowsEvent.Row>> res = uidRowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
        Assert.assertFalse(res.isNoRowFiltered().noRowFiltered());
        Assert.assertEquals(res.getRes().size(), 1);  // 18

        UidConfiguration.getInstance().clear();
    }

    @Test
    public void filterRowsWithWhiteList() throws Exception {
        List<RowsFilterConfig> rowsFilterConfigList = dataMediaConfig.getRowsFilters();
        RowsFilterConfig rowsFilterConfig = rowsFilterConfigList.get(0);
        RowsFilterConfig clone = getRowsFilterConfig(rowsFilterConfig, FetchMode.WhiteList.getCode());

        // white 18,20,21
        UidRowsFilterRule uidRowsFilterRule = new UidRowsFilterRule(clone);
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        RowsFilterResult<List<AbstractRowsEvent.Row>> res = uidRowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
        Assert.assertFalse(res.isNoRowFiltered().noRowFiltered());
        Assert.assertEquals(res.getRes().size(), 2);  // 18, 20

        UidConfiguration.getInstance().clear();
    }

    private RowsFilterConfig getRowsFilterConfig(RowsFilterConfig rowsFilterConfig, int code) {
        RowsFilterConfig clone = new RowsFilterConfig();
        clone.setMode(rowsFilterConfig.getMode());
        clone.setTables(rowsFilterConfig.getTables());
        clone.setRegistryKey(rowsFilterConfig.getRegistryKey());
        RowsFilterConfig.Parameters parameters = rowsFilterConfig.getParameters();

        RowsFilterConfig.Parameters cloneParameters = new RowsFilterConfig.Parameters();
        cloneParameters.setColumns(parameters.getColumns());
        cloneParameters.setContext(parameters.getContext());
        cloneParameters.setIllegalArgument(parameters.getIllegalArgument());
        cloneParameters.setFetchMode(code);

        RowsFilterConfig.Configs configs = new RowsFilterConfig.Configs();
        configs.setParameters(Lists.newArrayList(cloneParameters));
        clone.setConfigs(configs);

        return clone;
    }

}
