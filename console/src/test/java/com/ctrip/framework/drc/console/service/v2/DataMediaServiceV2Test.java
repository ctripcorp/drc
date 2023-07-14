package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.console.service.v2.impl.DataMediaServiceV2Impl;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
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

/**
 * Created by dengquanliang
 * 2023/5/30 20:18
 */
public class DataMediaServiceV2Test {

    @InjectMocks
    private DataMediaServiceV2Impl dataMediaService;
    @Mock
    private ColumnsFilterServiceV2 columnsFilterService;
    @Mock
    private RowsFilterServiceV2 rowsFilterService;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbTblDao dbTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGenerateConfig() throws SQLException {
        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(Lists.newArrayList(new DbReplicationFilterMappingTbl()));
        Mockito.when(columnsFilterService.generateColumnsFilterConfig(Mockito.anyString(), Mockito.anyList())).thenReturn(Lists.newArrayList(new ColumnsFilterConfig()));
        Mockito.when(rowsFilterService.generateRowsFiltersConfig(Mockito.anyString(), Mockito.anyList())).thenReturn(Lists.newArrayList(new RowsFilterConfig()));

        DbReplicationDto dbReplicationDto = new DbReplicationDto();
        dbReplicationDto.setDbReplicationId(0L);
        DataMediaConfig dataMediaConfig = dataMediaService.generateConfig(Lists.newArrayList(dbReplicationDto));
        Assert.assertEquals(dataMediaConfig.getRowsFilters().size(), 1);
        Assert.assertEquals(dataMediaConfig.getColumnsFilters().size(), 1);
    }
}
