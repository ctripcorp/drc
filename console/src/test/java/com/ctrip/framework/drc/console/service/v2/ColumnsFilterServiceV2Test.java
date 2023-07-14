package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.enums.ColumnsFilterModeEnum;
import com.ctrip.framework.drc.console.service.v2.impl.ColumnsFilterServiceV2Impl;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.assertj.core.util.Lists;
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
 * 2023/5/30 19:41
 */
public class ColumnsFilterServiceV2Test {

    @InjectMocks
    private ColumnsFilterServiceV2Impl columnsFilterService;
    @Mock
    private ColumnsFilterTblV2Dao columnsFilterTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGenerateColumnsFilterConfig() throws SQLException {
        List<ColumnsFilterTblV2> columnsFilterTbls = new ArrayList<>();
        ColumnsFilterTblV2 columnsFilterTbl = new ColumnsFilterTblV2();
        columnsFilterTbl.setMode(ColumnsFilterModeEnum.INCLUDE.getCode());
        columnsFilterTbl.setColumns(JsonUtils.toJson(Lists.newArrayList("column")));
        columnsFilterTbls.add(columnsFilterTbl);

        Mockito.when(columnsFilterTblDao.queryByIds(Mockito.anyList())).thenReturn(columnsFilterTbls);
        List<ColumnsFilterConfig> result = columnsFilterService.generateColumnsFilterConfig("table", new ArrayList<>());
        Assert.assertEquals(result.size(), 1);
    }
}
