package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.dao.v2.RowsFilterTblV2Dao;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
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
    private RowsFilterTblV2Dao rowsFilterTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGenerateConfig() throws SQLException {
        Mockito.when(rowsFilterTblDao.queryByIds(Mockito.anyList())).thenReturn(buildRowsFilterTbls());
        List<RowsFilterConfig> result = rowsFilterService.generateRowsFiltersConfig("table", new ArrayList<>());
        Assert.assertEquals(result.size(), 1);
    }

    private List<RowsFilterTblV2> buildRowsFilterTbls() {
        List<RowsFilterTblV2> rowsFilterTbls = new ArrayList<>();
        RowsFilterTblV2 rowsFilterTbl = new RowsFilterTblV2();
        rowsFilterTbl.setConfigs(JsonUtils.toJson(new RowsFilterConfig.Configs()));
        rowsFilterTbl.setMode(RowsFilterModeEnum.JAVA_REGEX.getCode());

        rowsFilterTbls.add(rowsFilterTbl);
        return rowsFilterTbls;
    }
}
