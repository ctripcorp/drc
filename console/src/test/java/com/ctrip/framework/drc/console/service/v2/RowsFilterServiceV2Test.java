package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.service.v2.impl.RowsFilterServiceV2Impl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/30 19:25
 */
public class RowsFilterServiceV2Test {

    @InjectMocks
    private RowsFilterServiceV2Impl rowsFilterService;
    @Mock
    private RowsFilterTblDao rowsFilterTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGenerateConfig() throws SQLException {
        Mockito.when(rowsFilterTblDao.queryByIds(Mockito.anyList())).thenReturn(buildRowsFilterTbls());
        List<RowsFilterConfig> result = rowsFilterService.generateRowsFiltersConfig("table", new ArrayList<>());
        Assert.assertEquals(result.size(), 2);
    }

    private List<RowsFilterTbl> buildRowsFilterTbls() {
        List<RowsFilterTbl> rowsFilterTbls = new ArrayList<>();
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setConfigs(JsonUtils.toJson(new RowsFilterConfig.Configs()));

        RowsFilterTbl rowsFilterTbl1 = new RowsFilterTbl();
        rowsFilterTbl1.setParameters(JsonUtils.toJson(new RowsFilterConfig.Parameters()));

        rowsFilterTbls.add(rowsFilterTbl);
        rowsFilterTbls.add(rowsFilterTbl1);
        return rowsFilterTbls;
    }
}
