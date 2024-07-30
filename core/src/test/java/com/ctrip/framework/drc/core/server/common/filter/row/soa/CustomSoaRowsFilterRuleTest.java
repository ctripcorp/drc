package com.ctrip.framework.drc.core.server.common.filter.row.soa;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent.Row;
import com.ctrip.framework.drc.core.exception.DrcException;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig.Configs;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractEventTest;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status;
import com.ctrip.framework.drc.core.server.common.filter.service.CustomSoaService;
import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CustomSoaRowsFilterRuleTest extends AbstractEventTest  {
    
    private CustomSoaRowsFilterRule customSoaRowsFilterRule;
    
    @Mock
    private CustomSoaService customSoaService;
    
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        return RowsFilterType.SOA_Custom;
    }

    @Override
    protected String getContext() {
        return "{\\\"code\\\":32578,\\\"name\\\":\\\"DataSyncService\\\"}";
    }
    
    @Test
    public void testDoFilterRows() throws Exception {
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        rowsFilterContext.setSrcRegion("fra");
        rowsFilterContext.setDstRegion("sha");
        
        // case 1
        try {
            customSoaRowsFilterRule = new CustomSoaRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
            customSoaRowsFilterRule.setCustomSoaService(customSoaService);
            customSoaRowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
        } catch (DrcException e) {
            Assert.assertTrue(e.getMessage().contains("config multi column not support"));
        }

        // case 2
        try {
            Mockito.when(customSoaService.filter(Mockito.eq(32578),Mockito.eq("DataSyncService"),Mockito.any(CustomSoaRowFilterContext.class))).thenAnswer(
                    invocation -> {
                        CustomSoaRowFilterContext context = invocation.getArgument(2);
                        if (context.getColumnValue().equals("20")) {
                            return Status.No_Filtered;
                        } else {
                            return Status.Filtered;
                        }
                    }
            );
            RowsFilterConfig rowsFilterConfig = dataMediaConfig.getRowsFilters().get(0);
            Configs configs = rowsFilterConfig.getConfigs();
            configs.getParameterList().get(0).setColumns(Lists.newArrayList("id"));
            customSoaRowsFilterRule = new CustomSoaRowsFilterRule(rowsFilterConfig);
            customSoaRowsFilterRule.setCustomSoaService(customSoaService);

            RowsFilterResult<List<Row>> result = customSoaRowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
            Assert.assertEquals(1, result.getRes().size());

            result = customSoaRowsFilterRule.filterRows(updateRowsEvent, rowsFilterContext);
            Assert.assertEquals(0, result.getRes().size());

            result = customSoaRowsFilterRule.filterRows(deleteRowsEvent, rowsFilterContext);
            Assert.assertEquals(1, result.getRes().size());
        } catch (Exception e) {
            System.out.println(e);
            Assert.fail();
        }
        
        
    }

    
    
}